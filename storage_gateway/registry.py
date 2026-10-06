"""内容与副本登记表：内容寻址归一、副本放置记账、节点状态联动。

并发策略：注册表内部持有一把可重入锁，所有"检查 + 记账"复合操作
（提交去重、放置、上下线联动、计数读取）均在注册表锁内完成；
需要同时操作节点时，遵循"先注册表锁、后节点锁"的固定顺序，避免死锁。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .errors import (
    ChecksumMismatchError,
    ContentNotFoundError,
    ContentUnavailableError,
    EmptyContentError,
    NodeNotFoundError,
    UnauthorizedNodeError,
)
from .hashing import sha256_hex, verify_checksum
from .models import ContentMetadata, NodeStatus, RetrievabilityResult
from .node import StorageNode


@dataclass
class _ContentRecord:
    """注册表内部可变记录（data 供修复时作为唯一可信样本来读取）。"""

    cid: str
    size: int
    checksum: str
    data: bytes


class ContentRegistry:
    def __init__(self, retrieval_threshold: int = 2) -> None:
        if retrieval_threshold < 1:
            raise ValueError("retrieval_threshold must be >= 1")
        self._threshold = retrieval_threshold
        self._nodes: dict[str, StorageNode] = {}
        self._contents: dict[str, _ContentRecord] = {}
        # 已被判定损坏的副本：cid -> 节点 id 集合。
        # 一旦登记，可检索性判定与修复都不再把它当作健康可用副本，
        # 直到该副本被移除（修复替换）为止。
        self._corrupt: dict[str, set[str]] = {}
        self._lock = threading.RLock()

    # ---- 节点管理 ----
    def register_node(self, node: StorageNode) -> None:
        with self._lock:
            if node.node_id in self._nodes:
                raise ValueError(f"node {node.node_id} already registered")
            self._nodes[node.node_id] = node

    def get_node(self, node_id: str) -> StorageNode:
        with self._lock:
            try:
                return self._nodes[node_id]
            except KeyError:
                raise NodeNotFoundError(f"node {node_id} not found") from None

    def list_nodes(self) -> tuple[StorageNode, ...]:
        """返回当前已注册节点的快照（顺序由调用方自行决定）。"""
        with self._lock:
            return tuple(self._nodes.values())

    def lock(self):
        """暴露注册表串行化锁，供同包协作方（如修复器）做复合原子操作。"""
        return self._lock

    def set_node_status(self, node_id: str, status: NodeStatus) -> None:
        """节点上下线。

        副本数据本身不删除，因此总副本数不变；可检索性（在线副本数）
        在后续判定时按节点当前状态实时计算，状态天然同步、无需修补。
        """
        with self._lock:
            node = self.get_node(node_id)
            node.set_status(status)

    # ---- 内容寻址 ----
    def submit_content(
        self, data: bytes, declared_checksum: str | None = None
    ) -> ContentMetadata:
        """提交内容并归一到 CID。

        - 空内容拒绝；
        - 若提供声明摘要但与实际 SHA-256 不一致，判定为内容损坏，拒绝；
        - 相同内容（相同 CID）重复提交返回同一元数据，不产生任何副本副作用。
        """
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")
        data = bytes(data)
        if len(data) == 0:
            raise EmptyContentError("empty content is not accepted")
        if declared_checksum is not None and not verify_checksum(
            data, declared_checksum
        ):
            raise ChecksumMismatchError(
                f"declared checksum {declared_checksum!r} does not match "
                f"actual sha256 {sha256_hex(data)}"
            )
        cid = sha256_hex(data)
        with self._lock:
            record = self._contents.get(cid)
            if record is None:
                self._contents[cid] = _ContentRecord(
                    cid=cid, size=len(data), checksum=cid, data=data
                )
            return self._snapshot_metadata(cid)

    def get_metadata(self, cid: str) -> ContentMetadata:
        with self._lock:
            self._require_content(cid)
            return self._snapshot_metadata(cid)

    def content_data(self, cid: str) -> bytes:
        """返回注册表保存的可信内容样本（修复来源）。"""
        with self._lock:
            return self._require_content(cid).data

    # ---- 内容读取 ----
    def retrieve_content(self, cid: str) -> bytes:
        """按内容标识从在线健康副本取回原始字节。

        - CID 从未提交过 → ContentNotFoundError；
        - 依次尝试在线且未被判定损坏的副本，逐份校验"字节摘要 == CID"：
          校验通过即返回；校验失败则把该副本登记为损坏并换下一份；
        - 所有在线副本都损坏（或根本没有在线副本）→ ContentUnavailableError。

        候选节点按 id 排序逐个尝试，同一份内容重复读取结果稳定；
        整个"读取 + 校验 + 损坏登记"在注册表锁内完成，与修复/上下线串行化。
        """
        with self._lock:
            self._require_content(cid)
            known_corrupt = self._corrupt.get(cid, set())
            candidates = sorted(
                (
                    n
                    for n in self._nodes.values()
                    if n.status is NodeStatus.ONLINE
                    and n.has(cid)
                    and n.node_id not in known_corrupt
                ),
                key=lambda n: n.node_id,
            )
            for node in candidates:
                data = node.read(cid)
                if sha256_hex(data) == cid:
                    return data
                # 字节与内容标识对不上：认定损坏并登记，绝不返回坏数据
                self._mark_corrupt(cid, node.node_id)
            total = sum(1 for n in self._nodes.values() if n.has(cid))
            corrupt = self._corrupt.get(cid, set())
            raise ContentUnavailableError(
                f"content {cid} is recorded but has no healthy online replica "
                f"(total replicas {total}, known corrupt {len(corrupt)})"
            )

    def corrupt_nodes_for(self, cid: str) -> tuple[str, ...]:
        """返回该内容已被判定损坏的副本所在节点 id（排序快照）。"""
        with self._lock:
            self._require_content(cid)
            return tuple(sorted(self._corrupt.get(cid, ())))

    def _mark_corrupt(self, cid: str, node_id: str) -> None:
        self._corrupt.setdefault(cid, set()).add(node_id)

    def _unmark_corrupt(self, cid: str, node_id: str) -> None:
        marked = self._corrupt.get(cid)
        if marked is None:
            return
        marked.discard(node_id)
        if not marked:
            del self._corrupt[cid]

    def list_content_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._contents.keys())

    # ---- 副本放置 ----
    def place_replica(self, cid: str, node_id: str) -> None:
        """在指定节点放置内容副本。

        检查（内容存在 / 节点存在 / 已授权）在注册表锁内完成，
        随后在同一把锁内调用节点放置，保证"检查 + 记账"原子。
        """
        with self._lock:
            record = self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            if not node.authorized:
                raise UnauthorizedNodeError(
                    f"node {node_id} is not authorized to store content"
                )
            # 节点自身继续对 离线 / 重复 / 容量不足 / 字节不符 做显式拒绝
            node.place(cid, record.size, record.data)

    def remove_replica(self, cid: str, node_id: str) -> None:
        with self._lock:
            record = self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            node.remove(cid, record.size)
            # 副本被移除后，其损坏登记一并清除（若日后重新放置则为全新副本）
            self._unmark_corrupt(cid, node_id)

    # ---- 计数与可检索性 ----
    def total_replica_count(self, cid: str) -> int:
        with self._lock:
            self._require_content(cid)
            return sum(1 for n in self._nodes.values() if n.has(cid))

    def online_replica_count(self, cid: str) -> int:
        """在线且未被判定损坏的健康副本数（损坏副本不计入）。"""
        with self._lock:
            self._require_content(cid)
            corrupt = self._corrupt.get(cid, set())
            return sum(
                1
                for n in self._nodes.values()
                if n.status is NodeStatus.ONLINE
                and n.has(cid)
                and n.node_id not in corrupt
            )

    def healthy_replica_count(self, cid: str) -> int:
        """有效（未损坏）副本总数：在线与离线节点上的好副本都计入。

        这是"是否还需要补副本"的口径：暂时离线但数据完好的副本仍算数，
        只要该口径已经达到目标份数，修复就不再新增副本，避免节点抖动
        恢复后有效副本数超出目标。
        """
        with self._lock:
            self._require_content(cid)
            corrupt = self._corrupt.get(cid, set())
            return sum(
                1
                for n in self._nodes.values()
                if n.has(cid) and n.node_id not in corrupt
            )

    def offline_healthy_nodes_for(self, cid: str) -> tuple[str, ...]:
        """持有有效好副本、但节点当前离线的节点 id（排序快照）。"""
        with self._lock:
            self._require_content(cid)
            corrupt = self._corrupt.get(cid, set())
            return tuple(
                sorted(
                    n.node_id
                    for n in self._nodes.values()
                    if n.status is NodeStatus.OFFLINE
                    and n.has(cid)
                    and n.node_id not in corrupt
                )
            )

    def check_retrievability(self, cid: str) -> RetrievabilityResult:
        """按"在线健康副本数 >= 阈值"判定可检索性并给出原因。

        已被判定损坏的副本不计入在线健康副本数，并在结论中单独说明。
        """
        with self._lock:
            self._require_content(cid)
            total = self.total_replica_count(cid)
            online = self.online_replica_count(cid)
            healthy = self.healthy_replica_count(cid)
            offline_healthy = healthy - online
            corrupt_count = len(self._corrupt.get(cid, ()))
            retrievable = online >= self._threshold
            reasons: list[str] = []
            if total == 0:
                reasons.append("内容尚无任何副本")
            elif online == 0:
                reasons.append(f"全部 {total} 个副本所在节点均离线或副本已损坏")
            elif online < self._threshold:
                reasons.append(
                    f"在线副本数 {online} 低于检索阈值 {self._threshold}"
                    f"（总副本数 {total}）"
                )
            # 有效（含离线）好副本已经达到目标份数时，即使在线数不足也不再
            # 需要补副本：节点恢复后有效副本数仍在目标内，修复不得超量。
            if (
                not retrievable
                and healthy >= self._threshold
                and offline_healthy > 0
            ):
                reasons.append(
                    f"另有 {offline_healthy} 个有效副本所在节点暂时离线，"
                    f"有效副本数 {healthy} 已达到目标份数 {self._threshold}，无需补副本"
                )
            if corrupt_count:
                reasons.append(f"另有 {corrupt_count} 个副本已被判定损坏，不计入健康副本")
            if retrievable:
                reasons = [
                    f"在线副本数 {online} >= 检索阈值 {self._threshold}"
                ]
                if corrupt_count:
                    reasons.append(
                        f"另有 {corrupt_count} 个副本已被判定损坏，不计入健康副本"
                    )
            return RetrievabilityResult(
                cid=cid,
                retrievable=retrievable,
                online_replica_count=online,
                total_replica_count=total,
                threshold=self._threshold,
                reasons=tuple(reasons),
                corrupt_replica_count=corrupt_count,
                healthy_replica_count=healthy,
            )

    def total_replicas_system_wide(self) -> int:
        """系统内所有节点副本数之和，供测试断言守恒。"""
        with self._lock:
            return sum(n.replica_count() for n in self._nodes.values())

    def place_on_first_eligible_node(
        self, cid: str, candidate_ids: tuple[str, ...]
    ) -> str | None:
        """在候选节点中原子地挑选第一个合规节点并放置副本。

        "筛选 + 放置"在同一把注册表锁内完成：并发修复/去重时，一个节点
        一旦被先到者占用，后到者会看到它已持有该副本而跳过，因此不会
        重复复制。返回实际放置的节点 id；无节点可放时返回 None。
        """
        with self._lock:
            record = self._require_content(cid)
            for node_id in candidate_ids:
                node = self._nodes.get(node_id)
                if node is None or not node.authorized:
                    continue
                if node.status is NodeStatus.OFFLINE:
                    continue
                if node.has(cid):
                    continue
                if node.free_bytes() < record.size:
                    continue
                node.place(cid, record.size, record.data)
                return node.node_id
            return None

    # ---- 内部辅助 ----
    def _require_content(self, cid: str) -> _ContentRecord:
        try:
            return self._contents[cid]
        except KeyError:
            raise ContentNotFoundError(f"content {cid} not found") from None

    def _snapshot_metadata(self, cid: str) -> ContentMetadata:
        return ContentMetadata(
            cid=cid,
            size=self._contents[cid].size,
            checksum=self._contents[cid].checksum,
            replica_nodes=frozenset(
                n.node_id for n in self._nodes.values() if n.has(cid)
            ),
        )
