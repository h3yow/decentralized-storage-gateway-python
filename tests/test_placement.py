"""副本放置：容量不足、重复放置、未授权、离线、未知节点均可区分拒绝。"""

from __future__ import annotations

import pytest

from storage_gateway.errors import (
    CapacityExceededError,
    DuplicatePlacementError,
    NodeOfflineError,
    NodeNotFoundError,
    UnauthorizedNodeError,
)
from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode

from .conftest import log_case


def _setup(registry):
    registry.register_node(StorageNode("n1", capacity=100))
    registry.register_node(StorageNode("n2", capacity=100))
    registry.register_node(StorageNode("n3", capacity=100, authorized=False))
    registry.register_node(StorageNode("n4", capacity=5))
    return registry.submit_content(b"x" * 50).cid


def test_duplicate_placement_rejected(registry):
    cid = _setup(registry)
    registry.place_replica(cid, "n1")
    log_case("重复放置", cid=cid[:8], node="n1", replicas_before=1)
    with pytest.raises(DuplicatePlacementError):
        registry.place_replica(cid, "n1")
    assert registry.total_replica_count(cid) == 1


def test_capacity_exceeded_rejected(registry):
    cid = _setup(registry)
    log_case("容量不足", node="n4", free=5, need=50)
    with pytest.raises(CapacityExceededError):
        registry.place_replica(cid, "n4")
    assert registry.total_replica_count(cid) == 0
    assert registry.get_node("n4").used_bytes == 0  # 拒绝不得产生记账


def test_unauthorized_node_rejected(registry):
    cid = _setup(registry)
    log_case("未授权节点", node="n3", authorized=False)
    with pytest.raises(UnauthorizedNodeError):
        registry.place_replica(cid, "n3")
    assert registry.total_replica_count(cid) == 0


def test_offline_node_rejected_for_new_placement(registry):
    cid = _setup(registry)
    registry.set_node_status("n2", NodeStatus.OFFLINE)
    log_case("离线放置", node="n2")
    with pytest.raises(NodeOfflineError):
        registry.place_replica(cid, "n2")
    assert registry.total_replica_count(cid) == 0


def test_unknown_node_rejected(registry):
    cid = _setup(registry)
    log_case("未知节点", node="ghost")
    with pytest.raises(NodeNotFoundError):
        registry.place_replica(cid, "ghost")


def test_gateway_replicate_reports_per_node_reason(gateway):
    reg = gateway.registry
    reg.register_node(StorageNode("n1", 100))
    reg.register_node(StorageNode("n2", 100, authorized=False))
    cid = gateway.submit(b"hello-gateway").cid
    results = gateway.replicate(cid, ["n1", "n2", "missing"])
    log_case("批量复制结果", results=results)
    assert results["n1"] == "ok"
    assert "UnauthorizedNodeError" in results["n2"]
    assert "NodeNotFoundError" in results["missing"]
