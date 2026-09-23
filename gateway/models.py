"""数据模型（骨架）。"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class ContentStatus(str, Enum):
    RETRIEVABLE = "retrievable"
    UNRETRIEVABLE = "unretrievable"


@dataclass
class NodeInfo:
    node_id: str
    capacity: int
    authorized: bool = True
    status: NodeStatus = NodeStatus.ONLINE
    used: int = 0


@dataclass
class ReplicaRecord:
    cid: str
    node_id: str
    size: int


@dataclass
class ContentRecord:
    cid: str
    size: int
    checksum: str
    status: ContentStatus = ContentStatus.UNRETRIEVABLE
    replicas: dict[str, ReplicaRecord] = field(default_factory=dict)
