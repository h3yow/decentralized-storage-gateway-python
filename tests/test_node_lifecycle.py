"""节点上下线：副本总数守恒，在线副本数与可检索性同步更新。"""

from __future__ import annotations

from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case


def test_offline_keeps_total_but_drops_online_count(registry):
    registry.register_node(StorageNode("n1", 100))
    registry.register_node(StorageNode("n2", 100))
    cid = registry.submit_content(b"lifecycle").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")

    registry.set_node_status("n1", NodeStatus.OFFLINE)
    res = registry.check_retrievability(cid)
    log_case(
        "单节点下线",
        total=res.total_replica_count,
        online=res.online_replica_count,
        retrievable=res.retrievable,
        reason=res.reasons,
    )
    assert res.total_replica_count == 2          # 总量守恒
    assert res.online_replica_count == 1
    assert not res.retrievable

    registry.set_node_status("n1", NodeStatus.ONLINE)
    res2 = registry.check_retrievability(cid)
    log_case("节点恢复", online=res2.online_replica_count, retrievable=res2.retrievable)
    assert res2.online_replica_count == 2
    assert res2.retrievable


def test_all_offline_then_repair_blocked_until_source_back(registry):
    registry.register_node(StorageNode("n1", 100))
    registry.register_node(StorageNode("n2", 100))
    cid = registry.submit_content(b"only").cid
    registry.place_replica(cid, "n1")
    registry.set_node_status("n1", NodeStatus.OFFLINE)
    rep = RepairManager(registry).repair(cid)
    log_case("全离线修复", repaired=rep.repaired, reason=rep.reason)
    assert not rep.repaired
    assert registry.total_replica_count(cid) == 1  # 唯一样本保留
