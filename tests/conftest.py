"""测试公共夹具与日志工具：每个用例打印输入与判定依据。"""

from __future__ import annotations

import logging

import pytest

from storage_gateway.gateway import StorageGateway
from storage_gateway.node import StorageNode
from storage_gateway.registry import ContentRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("storage-tests")


def log_case(title: str, **fields) -> None:
    parts = " | ".join(f"{k}={v!r}" for k, v in fields.items())
    logger.info("用例[%s] 输入/依据 -> %s", title, parts)


@pytest.fixture()
def registry() -> ContentRegistry:
    return ContentRegistry(retrieval_threshold=2)


@pytest.fixture()
def gateway() -> StorageGateway:
    return StorageGateway(retrieval_threshold=2)


def make_nodes(reg: ContentRegistry, spec: dict[str, int]) -> None:
    """按 {node_id: capacity} 批量注册默认授权在线节点。"""
    for node_id, cap in spec.items():
        reg.register_node(StorageNode(node_id, cap))
