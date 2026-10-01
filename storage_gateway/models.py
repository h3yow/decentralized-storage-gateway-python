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
    """可检索性判定结论。

    online_replica_count 只统计"在线且字节健康"的副本；
    online_corrupted_count 统计在线但字节与 CID 对不上、已被隔离的坏副本。
    """

    cid: str
    retrievable: bool
    online_replica_count: int
    total_replica_count: int
    threshold: int
    reasons: tuple[str, ...] = ()
    online_corrupted_count: int = 0


@dataclass(frozen=True)
class RepairReport:
    """单次修复尝试的结论说明。

    replaced_nodes: 修复期间用健康样本重写替换掉坏副本的节点；
    target_nodes: 修复期间新增副本的节点；
    valid_replica_count: 修复结束后健康有效副本总数（含离线节点上的健康副本），
    该值不会超过修复目标阈值。
    """

    cid: str
    repaired: bool
    before_online_count: int
    after_online_count: int
    target_nodes: tuple[str, ...] = ()
    reason: str = ""
    replaced_nodes: tuple[str, ...] = ()
    valid_replica_count: int = 0
