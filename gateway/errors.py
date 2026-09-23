"""失败原因与异常定义（骨架）。"""

from __future__ import annotations

from enum import Enum


class FailureReason(str, Enum):
    """可区分的失败原因。"""

    CHECKSUM_MISMATCH = "checksum_mismatch"
    CONTENT_CORRUPTED = "content_corrupted"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    DUPLICATE_PLACEMENT = "duplicate_placement"
    UNAUTHORIZED_NODE = "unauthorized_node"
    NODE_OFFLINE = "node_offline"
    NODE_NOT_FOUND = "node_not_found"
    CONTENT_NOT_FOUND = "content_not_found"
    INSUFFICIENT_REPLICAS = "insufficient_replicas"
    NO_REPAIR_SOURCE = "no_repair_source"


class GatewayError(Exception):
    """携带可区分原因的业务异常。"""

    def __init__(self, reason: FailureReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")
