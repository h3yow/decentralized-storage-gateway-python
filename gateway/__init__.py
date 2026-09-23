"""本地去中心化存储网关：内容寻址、副本管理、可检索性判定与修复。"""

from .errors import FailureReason, GatewayError
from .models import ContentRecord, ContentStatus, NodeInfo, NodeStatus, ReplicaRecord
from .node import LocalStorageNode
from .gateway import GatewayConfig, StorageGateway

__all__ = [
    "FailureReason",
    "GatewayError",
    "ContentRecord",
    "ContentStatus",
    "NodeInfo",
    "NodeStatus",
    "ReplicaRecord",
    "LocalStorageNode",
    "GatewayConfig",
    "StorageGateway",
]
