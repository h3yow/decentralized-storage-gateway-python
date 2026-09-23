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


@dataclass(frozen=True)
class RepairReport:
    """单次修复尝试的结论说明。"""

    cid: str
    repaired: bool
    before_online_count: int
    after_online_count: int
    target_nodes: tuple[str, ...] = ()
    reason: str = ""
