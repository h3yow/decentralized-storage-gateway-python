"""存储网关核心：内容寻址、副本放置、可检索性判定与修复。

线程模型：所有“检查并执行”的复合操作都在网关级可重入锁内完成，
节点内部另有锁保护字节存取与用量计数，保证并发放置、去重、上下线、
修复不会产生重复副本或计数错乱。
"""

from __future__ import annotations

import hashlib
import logging
import threading
from dataclasses import dataclass

from .errors import FailureReason, GatewayError
from .models import ContentRecord, ContentStatus, NodeInfo, NodeStatus, ReplicaRecord
from .node import LocalStorageNode

logger = logging.getLogger("storage_gateway")


@dataclass
class GatewayConfig:
    """可配置项。"""

    min_replicas: int = 2
    checksum_algo: str = "sha256"

    def __post_init__(self) -> None:
        if self.min_replicas < 1:
            raise ValueError("min_replicas must be >= 1")


class StorageGateway:
    def __init__(self, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig()
        self._lock = threading.RLock()
        self._nodes: dict[str, LocalStorageNode] = {}
        self._contents: dict[str, ContentRecord] = {}
        self._canonical: dict[str, bytes] = {}

    # ---- 内容寻址基础 ----
    def _digest(self, data: bytes) -> str:
        return hashlib.new(self.config.checksum_algo, data).hexdigest()

    def _record(self, cid: str) -> ContentRecord:
        try:
            return self._contents[cid]
        except KeyError:
            raise GatewayError(FailureReason.CONTENT_NOT_FOUND, f"unknown content {cid}") from None

    def _node(self, node_id: str) -> LocalStorageNode:
        try:
            return self._nodes[node_id]
        except KeyError:
            raise GatewayError(FailureReason.NODE_NOT_FOUND, f"unknown node {node_id}") from None

    # ---- 节点管理 ----
    def register_node(self, node: LocalStorageNode) -> None:
        with self._lock:
            if node.node_id in self._nodes:
                raise ValueError(f"node {node.node_id} already registered")
            self._nodes[node.node_id] = node
            logger.info("register_node input node=%s capacity=%s authorized=%s",
                        node.node_id, node.info.capacity, node.info.authorized)

    def set_node_status(self, node_id: str, online: bool) -> None:
        with self._lock:
            node = self._node(node_id)
            if online:
                node.go_online()
            else:
                node.go_offline()
            # 该节点上的副本可检索性同步更新
            for record in self._contents.values():
                if node_id in record.replicas:
                    self._refresh_status(record)
            logger.info(
                "set_node_status input node=%s online=%s -> affected contents re-evaluated",
                node_id, online,
            )

    def list_nodes(self) -> list[NodeInfo]:
        with self._lock:
            return [n.info for n in self._nodes.values()]

    # ---- 内容提交（寻址 + 去重） ----
    def submit(self, data: bytes, expected_checksum: str | None = None) -> str:
        checksum = self._digest(data)
        logger.info("submit input size=%s expected_checksum=%s computed=%s",
                    len(data), expected_checksum, checksum)
        if expected_checksum is not None and expected_checksum != checksum:
            logger.warning("submit rejected reason=%s expected=%s actual=%s",
                           FailureReason.CHECKSUM_MISMATCH.value, expected_checksum, checksum)
            raise GatewayError(
                FailureReason.CHECKSUM_MISMATCH,
                f"expected {expected_checksum}, actual {checksum}",
            )
        with self._lock:
            existing = self._contents.get(checksum)
            if existing is not None:
                # 相同内容归一到同一标识，不产生多余副本
                logger.info("submit deduped cid=%s replicas=%s (no new replica)",
                            checksum, len(existing.replicas))
                return checksum
            record = ContentRecord(cid=checksum, size=len(data), checksum=checksum)
            self._contents[checksum] = record
            self._canonical[checksum] = bytes(data)
            self._refresh_status(record)
            logger.info("submit accepted cid=%s size=%s status=%s",
                        checksum, len(data), record.status.value)
            return checksum

    def get(self, cid: str) -> bytes:
        with self._lock:
            self._record(cid)
            for node in self._nodes.values():
                if node.has(cid) and node.status.value == "online":
                    data = node.fetch(cid)
                    self._verify(cid, data)
                    return data
            # 没有在线副本时，回退到网关持有的权威样本
            data = self._canonical[cid]
            self._verify(cid, data)
            return data

    def _verify(self, cid: str, data: bytes) -> None:
        actual = self._digest(data)
        if actual != cid:
            raise GatewayError(
                FailureReason.CONTENT_CORRUPTED,
                f"stored bytes of {cid} digest to {actual}",
            )

    # ---- 副本放置 ----
    def place_replica(self, cid: str, node_id: str) -> None:
        with self._lock:
            record = self._record(cid)
            node = self._node(node_id)
            if node_id in record.replicas:
                raise GatewayError(
                    FailureReason.DUPLICATE_PLACEMENT,
                    f"{cid} already placed on node {node_id}",
                )
            data = self._canonical[cid]
            # node.store 内部会区分：未授权 / 离线 / 容量不足 / 重复
            node.store(cid, data)
            record.replicas[node_id] = ReplicaRecord(cid=cid, node_id=node_id, size=len(data))
            self._refresh_status(record)
            logger.info(
                "place_replica cid=%s node=%s online_replicas=%s status=%s",
                cid, node_id, self._online_replica_count(record), record.status.value,
            )

    # ---- 可检索性判定 ----
    def _online_replica_count(self, record: ContentRecord) -> int:
        return sum(
            1
            for nid in record.replicas
            if nid in self._nodes and self._nodes[nid].status is NodeStatus.ONLINE
        )

    def _refresh_status(self, record: ContentRecord) -> None:
        online = self._online_replica_count(record)
        record.status = (
            ContentStatus.RETRIEVABLE
            if online >= self.config.min_replicas
            else ContentStatus.UNRETRIEVABLE
        )
        logger.info(
            "refresh_status cid=%s online_replicas=%s min=%s -> %s",
            record.cid, online, self.config.min_replicas, record.status.value,
        )

    def retrievability(self, cid: str) -> ContentStatus:
        with self._lock:
            record = self._record(cid)
            self._refresh_status(record)
            return record.status

    def replica_count(self, cid: str, online_only: bool = True) -> int:
        with self._lock:
            record = self._record(cid)
            if not online_only:
                return len(record.replicas)
            return self._online_replica_count(record)

    def total_replicas(self, online_only: bool = True) -> int:
        with self._lock:
            return sum(
                self.replica_count(record.cid, online_only=online_only)
                for record in self._contents.values()
            )

    # ---- 修复 ----
    def repair(self, cid: str) -> ContentStatus:
        with self._lock:
            record = self._record(cid)
            online = self._online_replica_count(record)
            logger.info("repair start cid=%s online_replicas=%s min=%s",
                        cid, online, self.config.min_replicas)
            if online >= self.config.min_replicas:
                logger.info("repair skipped cid=%s reason=already_retrievable", cid)
                return record.status

            # 1) 寻找一个在线且字节完好的样本源
            source = self._find_good_source(record)
            if source is None:
                raise GatewayError(
                    FailureReason.NO_REPAIR_SOURCE,
                    f"no online intact replica of {cid} to copy from",
                )

            # 2) 向满足条件的节点复制，直到达到阈值；只增不删，唯一样本永不丢失
            target = self.config.min_replicas
            last_reason: str | None = None
            for node in self._candidate_nodes(record):
                if self._online_replica_count(record) >= target:
                    break
                try:
                    node.store(cid, source)
                except GatewayError as exc:
                    last_reason = exc.reason.value
                    logger.info("repair candidate skipped node=%s reason=%s",
                                node.node_id, exc.reason.value)
                    continue
                record.replicas[node.node_id] = ReplicaRecord(
                    cid=cid, node_id=node.node_id, size=len(source)
                )
                logger.info("repair copied cid=%s -> node=%s", cid, node.node_id)

            self._refresh_status(record)
            if record.status is ContentStatus.UNRETRIEVABLE:
                # 修复失败：已放置的副本保留（原状态不被破坏），并说明原因
                raise GatewayError(
                    FailureReason.INSUFFICIENT_REPLICAS,
                    f"cannot reach {target} replicas, last_blocker={last_reason}",
                )
            logger.info("repair succeeded cid=%s status=%s", cid, record.status.value)
            return record.status

    def _find_good_source(self, record: ContentRecord) -> bytes | None:
        for nid, node in self._nodes.items():
            if nid not in record.replicas or node.status is not NodeStatus.ONLINE:
                continue
            data = node.fetch(record.cid)
            if self._digest(data) == record.checksum:
                return data
            logger.warning("repair source skipped node=%s reason=%s",
                           nid, FailureReason.CONTENT_CORRUPTED.value)
        return None

    def _candidate_nodes(self, record: ContentRecord) -> list[LocalStorageNode]:
        return [
            node
            for nid, node in self._nodes.items()
            if node.status is NodeStatus.ONLINE
            and node.info.authorized
            and nid not in record.replicas
        ]

    # ---- 故障注入（测试与可解释性演示用） ----
    def simulate_corruption(self, node_id: str, cid: str, bad: bytes) -> None:
        with self._lock:
            node = self._node(node_id)
            if not node.has(cid):
                raise GatewayError(FailureReason.CONTENT_NOT_FOUND,
                                   f"{cid} not on node {node_id}")
            with node._lock:
                node._data[cid] = bytes(bad)
                node.info.used = sum(len(v) for v in node._data.values())
            logger.warning("simulate_corruption node=%s cid=%s new_size=%s",
                           node_id, cid, len(bad))
