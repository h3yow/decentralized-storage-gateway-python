"""核心数据模型：内容元数据、节点状态、可检索性判定结果。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


@dataclass(frozen=True)
class ContentMetadata:
    """内容寻址元数据：以 SHA-256 摘要作为内容标识 CID。"""

    cid: str
    size: int
    checksum: str
    replica_nodes: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class RetrievabilityResult:
    """可检索性判定结论。"""

    cid: str
    retrievable: bool
    online_replica_count: int
    total_replica_count: int
    threshold: int
    reasons: tuple[str, ...] = ()
    # 已被判定损坏（字节与 CID 不符）的副本数；不计入在线健康副本
    corrupt_replica_count: int = 0


@dataclass(frozen=True)
class RepairReport:
    """单次修复尝试的结论说明。"""

    cid: str
    repaired: bool
    before_online_count: int
    after_online_count: int
    target_nodes: tuple[str, ...] = ()
    reason: str = ""
    # 被移除的损坏副本所在节点（这些节点上的坏副本已被换下）
    replaced_corrupt_nodes: tuple[str, ...] = ()
    # 修复结束后持有有效（未损坏）副本的全部节点
    valid_nodes: tuple[str, ...] = ()
    # 修复结束时的有效副本数（含暂时离线的健康副本）与目标份数：
    # 不变式为 after_effective_count <= target_count（修复自身绝不超量）
    after_effective_count: int = 0
    target_count: int = 0
