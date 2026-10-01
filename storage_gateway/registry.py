"""内容与副本登记表：内容寻址归一、副本放置记账、节点状态联动、坏副本隔离与取回。

并发策略：注册表内部持有一把可重入锁，所有"检查 + 记账"复合操作
（提交去重、放置、上下线联动、计数读取、坏副本判定、取回、修复替换）
均在注册表锁内完成；需要同时操作节点时，遵循"先注册表锁、后节点锁"的
固定顺序，避免死锁。

坏副本模型：某个在线节点上保存的字节若与其 CID（SHA-256）对不上，
该节点会被记入该 CID 的"损坏隔离集"。此后：
- 取回不会再读它，判定/计数不再把它算作健康副本，修复也不会以它为来源；
- 隔离标记会一直保留，直到该副本被正确字节就地重写（修复）或被移除。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .errors import (
    ChecksumMismatchError,
    ContentNotFoundError,
    EmptyContentError,
    NodeNotFoundError,
    ReplicaUnavailableError,
    UnauthorizedNodeError,
)
from .hashing import sha256_hex, verify_checksum
from .models import ContentMetadata, NodeStatus, RetrievabilityResult
from .node import StorageNode


@dataclass
class _ContentRecord:
    """注册表内部可变记录（data 为修复/取回的可信样本来源）。"""

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
        # cid -> 已被认定为"字节损坏"的节点 id 集合（跨在线/离线持续保留）
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
            # 节点自身继续对 离线 / 重复 / 容量不足 做显式拒绝
            node.place(cid, record.data)
            # 全新放置拿到的是注册表可信样本，该节点不再算损坏
            self._mark_healthy(cid, node_id)

    def remove_replica(self, cid: str, node_id: str) -> None:
        with self._lock:
            self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            node.remove(cid)
            self._corrupt.get(cid, set()).discard(node_id)

    # ---- 计数与可检索性 ----
    def total_replica_count(self, cid: str) -> int:
        """总副本数（含在线/离线节点，含已隔离的坏副本占位）。"""
        with self._lock:
            self._require_content(cid)
            return sum(1 for n in self._nodes.values() if n.has(cid))

    def online_replica_count(self, cid: str) -> int:
        """在线**健康**副本数：在线且字节校验通过、未被隔离的副本。

        统计前会对所有在线副本做字节校验，任何字节与 CID 对不上的副本
        都会被当场隔离，绝不计入健康数。
        """
        with self._lock:
            self._require_content(cid)
            return self._healthy_online(cid)[0]

    def online_corrupted_count(self, cid: str) -> int:
        """在线但已被隔离的坏副本数。"""
        with self._lock:
            self._require_content(cid)
            return self._healthy_online(cid)[1]

    def healthy_replica_count(self, cid: str) -> int:
        """全部节点（含离线）上的健康有效副本总数。"""
        with self._lock:
            self._require_content(cid)
            return sum(
                1
                for n in self._nodes.values()
                if n.has(cid) and n.node_id not in self._corrupt.get(cid, set())
            )

    def corrupted_nodes(self, cid: str) -> frozenset[str]:
        """当前被隔离为坏副本的节点集合快照。"""
        with self._lock:
            self._require_content(cid)
            return frozenset(self._corrupt.get(cid, set()))

    def check_retrievability(self, cid: str) -> RetrievabilityResult:
        """按"在线健康副本数 >= 阈值"判定可检索性并给出原因。"""
        with self._lock:
            self._require_content(cid)
            total = self.total_replica_count(cid)
            online, corrupted = self._healthy_online(cid)
            retrievable = online >= self._threshold
            reasons: list[str] = []
            if total == 0:
                reasons.append("内容尚无任何副本")
            elif online == 0:
                if corrupted > 0:
                    reasons.append(
                        f"在线的 {corrupted} 个副本字节均已损坏被隔离，"
                        f"无健康副本（总副本数 {total}）"
                    )
                else:
                    reasons.append(f"全部 {total} 个副本所在节点均离线")
            elif online < self._threshold:
                tail = f"（总副本数 {total}）"
                if corrupted:
                    tail = (
                        f"（另有 {corrupted} 个在线坏副本已隔离，"
                        f"总副本数 {total}）"
                    )
                reasons.append(
                    f"在线健康副本数 {online} 低于检索阈值 {self._threshold}{tail}"
                )
            if retrievable:
                reasons = [
                    f"在线健康副本数 {online} >= 检索阈值 {self._threshold}"
                ]
                if corrupted:
                    reasons.append(
                        f"另有 {corrupted} 个在线坏副本已隔离，不计入健康副本"
                    )
            return RetrievabilityResult(
                cid=cid,
                retrievable=retrievable,
                online_replica_count=online,
                total_replica_count=total,
                threshold=self._threshold,
                reasons=tuple(reasons),
                online_corrupted_count=corrupted,
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
                if node.status != NodeStatus.ONLINE:
                    continue
                if node.has(cid):
                    continue
                if node.free_bytes() < record.size:
                    continue
                node.place(cid, record.data)
                self._mark_healthy(cid, node_id)
                return node.node_id
            return None

    # ---- 取回、坏副本校验与替换 ----
    def retrieve_content(self, cid: str) -> bytes:
        """按内容标识取回与提交时完全一致的字节。

        只从**在线且字节校验通过**的副本取回，候选按节点 id 排序，
        结论确定、可复现：同一内容重复取回结果稳定。
        任何在线副本只要字节与其 CID 对不上，就当场隔离并转去尝试下一个，
        绝不把坏数据返回给调用方。

        - CID 从未提交过            -> ContentNotFoundError（404）
        - 有记录但无在线健康副本可取 -> ReplicaUnavailableError（503），
          原因中区分"无副本/在线全损坏/健康副本都在离线节点"。
        """
        with self._lock:
            record = self._require_content(cid)
            healthy, corrupted = self._healthy_online(cid)
            if healthy > 0:
                # _healthy_online 已对全部在线副本逐个校验过字节，
                # 这里按节点 id 顺序取第一份已验证健康的副本即可。
                for node in self._sorted_nodes():
                    if (
                        node.status == NodeStatus.ONLINE
                        and node.has(cid)
                        and node.node_id not in self._corrupt.get(cid, set())
                    ):
                        return node.read_replica(cid)
            # 无在线健康副本：给出可区分、可解释的失败原因
            total = sum(1 for n in self._nodes.values() if n.has(cid))
            offline_healthy = 0
            for n in self._nodes.values():
                if n.has(cid) and n.node_id not in self._corrupt.get(cid, set()):
                    if n.status != NodeStatus.ONLINE:
                        offline_healthy += 1
            if total == 0:
                detail = "内容尚无任何副本"
            elif corrupted > 0:
                bad = sorted(self._corrupt.get(cid, set()))
                detail = (
                    f"在线副本 {bad} 字节与 CID 不符，已全部隔离；"
                )
                if offline_healthy:
                    detail += f"另有 {offline_healthy} 个健康副本在离线节点上"
                else:
                    detail += "且没有其他健康副本可取"
            else:
                detail = (
                    f"健康副本所在节点均离线（{offline_healthy} 个），"
                    "当前没有在线健康副本可取"
                )
            raise ReplicaUnavailableError(
                f"content {cid} is currently unavailable: {detail}"
            )

    def verify_replica(self, cid: str, node_id: str) -> bool:
        """显式校验某节点上的副本字节是否与 CID 一致，并据此更新隔离标记。

        不区分节点在线/离线（磁盘内容不随上下线改变），供修复器与测试使用。
        """
        with self._lock:
            self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            if not node.has(cid):
                return False
            data = node.read_replica(cid)
            if sha256_hex(data) == cid:
                self._mark_healthy(cid, node_id)
                return True
            self._mark_corrupt(cid, node_id)
            return False

    def replace_corrupted_replica(self, cid: str, node_id: str) -> None:
        """用注册表可信样本就地重写某节点上的坏副本（修复专用）。

        仅替换节点上已存在的副本字节，不新增副本、不改变容量记账；
        重写字节必定与 CID 一致，成功后解除该节点的损坏隔离。
        """
        with self._lock:
            record = self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            if not node.has(cid):
                raise NodeNotFoundError(
                    f"content {cid} not found on node {node_id}"
                )
            node.rewrite_replica(cid, record.data)
            self._mark_healthy(cid, node_id)

    def tamper_replica(self, cid: str, node_id: str, bad_data: bytes) -> None:
        """故障注入：把某节点落盘字节改成与 CID 不符的内容（容量记账不变）。

        本方法只改字节、不预先下隔离结论；损坏由下一次读取/判定时的字节
        校验自动发现并隔离，以真实模拟"落盘数据被人改过"的场景。
        """
        with self._lock:
            self._require_content(cid)
            node = self._nodes.get(node_id)
            if node is None:
                raise NodeNotFoundError(f"node {node_id} not found")
            node.tamper_replica(cid, bad_data)

    # ---- 内部辅助 ----
    def _sorted_nodes(self) -> list[StorageNode]:
        return sorted(self._nodes.values(), key=lambda n: n.node_id)

    def _mark_corrupt(self, cid: str, node_id: str) -> None:
        self._corrupt.setdefault(cid, set()).add(node_id)

    def _mark_healthy(self, cid: str, node_id: str) -> None:
        self._corrupt.get(cid, set()).discard(node_id)

    def _healthy_online(self, cid: str) -> tuple[int, int]:
        """扫描全部在线副本并逐个做字节校验，返回 (健康数, 在线坏副本数)。

        校验结果实时写入/清理隔离集；调用方必须已持有注册表锁。
        节点按 id 排序扫描，保证并发下结论确定。
        """
        healthy = 0
        corrupted = 0
        corrupt_set = self._corrupt.setdefault(cid, set())
        for node in self._sorted_nodes():
            if not node.has(cid):
                # 副本已不存在（被移除），陈旧的隔离标记一并清掉
                corrupt_set.discard(node.node_id)
                continue
            # 无论节点在线/离线都复核字节：隔离标记必须始终与真实字节一致，
            # 避免"坏副本被修好/换盘恢复后仍被当成坏的"这类陈旧结论。
            data = node.read_replica(cid)
            bytes_ok = sha256_hex(data) == cid
            if bytes_ok:
                corrupt_set.discard(node.node_id)
            else:
                corrupt_set.add(node.node_id)
            if node.status == NodeStatus.ONLINE:
                if bytes_ok:
                    healthy += 1
                else:
                    corrupted += 1
        return healthy, corrupted

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
