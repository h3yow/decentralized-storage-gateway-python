"""可配置项：检索阈值、默认节点容量等。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GatewayConfig:
    """网关配置。

    retrieval_threshold: 判定可检索所需的最少在线健康副本数。
    default_node_capacity: 通过 HTTP 建节点且未显式指定容量时的默认容量（字节）。
    """

    retrieval_threshold: int = 2
    default_node_capacity: int = 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.retrieval_threshold < 1:
            raise ValueError("retrieval_threshold must be >= 1")
        if self.default_node_capacity < 0:
            raise ValueError("default_node_capacity must be >= 0")
