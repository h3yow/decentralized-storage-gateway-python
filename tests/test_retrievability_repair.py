"""可检索性判定与修复：阈值、补齐、失败保留原状态并说明原因。"""

from __future__ import annotations

from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case


def test_below_threshold_is_not_retrievable(registry):
    registry.register_node(StorageNode("n1", 100))
    cid = registry.submit_content(b"thin").cid
    registry.place_replica(cid, "n1")
    res = registry.check_retrievability(cid)
    log_case("低于阈值", online=res.online_replica_count, threshold=res.threshold, reasons=res.reasons)
    assert not res.retrievable
    assert "低于检索阈值" in res.reasons[0]


def test_repair_restores_threshold(registry):
    registry.register_node(StorageNode("n1", 100))
    registry.register_node(StorageNode("n2", 100))
    cid = registry.submit_content(b"fixme").cid
    registry.place_replica(cid, "n1")
    before_total = registry.total_replicas_system_wide()

    rep = RepairManager(registry).repair(cid)
    log_case("修复", before=rep.before_online_count, after=rep.after_online_count, targets=rep.target_nodes)
    assert rep.repaired
    assert registry.total_replica_count(cid) == 2
    assert registry.total_replicas_system_wide() == before_total + 1


def test_repair_failure_preserves_state(registry):
    registry.register_node(StorageNode("n1", 100))
    registry.register_node(StorageNode("n2", 10))  # 容量不足
    cid = registry.submit_content(b"x" * 50).cid
    registry.place_replica(cid, "n1")
    rep = RepairManager(registry).repair(cid)
    log_case("修复失败保留", repaired=rep.repaired, reason=rep.reason)
    assert not rep.repaired
    assert registry.total_replica_count(cid) == 1
    assert registry.online_replica_count(cid) == 1
    assert "没有合规" in rep.reason


def test_repair_is_idempotent_when_healthy(registry):
    registry.register_node(StorageNode("n1", 100))
    registry.register_node(StorageNode("n2", 100))
    cid = registry.submit_content(b"healthy").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    rm = RepairManager(registry)
    rep1 = rm.repair(cid)
    rep2 = rm.repair(cid)
    log_case("健康幂等", r1=rep1.repaired, r2=rep2.repaired)
    assert not rep1.repaired and not rep2.repaired
    assert registry.total_replica_count(cid) == 2
