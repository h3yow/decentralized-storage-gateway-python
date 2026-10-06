"""修复与节点上下线抖动交叉：有效副本数绝不超目标、记账守恒、坏副本被替换。

本文件聚焦"节点在修复期间反复上下线"这一曾导致超量复制的场景：
- 判定补副本时，暂时离线的好副本计入有效副本数，够目标就不再补；
- 修复结束后报告清楚列出有效副本节点、被换掉的坏副本节点与最终口径；
- "先撤坏副本、再补一份"不重复计费，读取不会把坏副本当结果。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from storage_gateway.hashing import sha256_hex
from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case

DATA = b"flap-repair-block"
THRESHOLD = 2


def _setup(registry, n: int = 4, data: bytes = DATA):
    for i in range(n):
        registry.register_node(StorageNode(f"n{i}", 10_000))
    cid = registry.submit_content(data).cid
    return cid


def _used_bytes_consistent(registry, size: int) -> bool:
    return all(node.used_bytes == node.replica_count() * size for node in registry.list_nodes())


def _assert_invariants(registry, cid: str, size: int, cap_total: int = THRESHOLD) -> None:
    """在注册表锁内取一致快照校验修复相关不变量。

    修复的"撤坏副本 + 补新副本"在同一把注册表锁内原子完成，因此校验也必须
    持同一把锁，避免在撤/补之间读到 used_bytes 与副本数对不上的撕裂中间态。
    """
    with registry.lock():
        assert registry.healthy_replica_count(cid) <= cap_total
        assert registry.total_replica_count(cid) <= cap_total
        for node in registry.list_nodes():
            assert node.used_bytes == node.replica_count() * size
            assert node.replica_count() <= 1


# ---- 确定性交织：离线好副本计入目标，不补副本 ----

def test_offline_healthy_replica_counts_toward_target(registry):
    cid = _setup(registry)
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")

    registry.set_node_status("n1", NodeStatus.OFFLINE)
    rep = RepairManager(registry).repair(cid)
    res = registry.check_retrievability(cid)
    log_case(
        "离线好副本计入目标",
        online=res.online_replica_count,
        healthy=res.healthy_replica_count,
        targets=rep.target_nodes,
        offline_valid=rep.offline_valid_nodes,
    )
    # 当前不可检索（在线 1 < 2），但有效（含离线）好副本已够目标
    assert not res.retrievable
    assert res.healthy_replica_count == THRESHOLD
    assert rep.target_nodes == ()
    assert rep.offline_valid_nodes == ("n1",)
    assert rep.healthy_replica_count == rep.target_replica_count
    assert "无需补副本" in rep.reason


def test_repair_during_offline_then_node_back_does_not_exceed_target(registry):
    """曾导致超量的主场景：好副本节点修复期间离线、修复后恢复。"""
    cid = _setup(registry)
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")

    registry.set_node_status("n1", NodeStatus.OFFLINE)
    rep = RepairManager(registry).repair(cid)
    assert rep.target_nodes == ()                 # 不补
    registry.set_node_status("n1", NodeStatus.ONLINE)

    total = registry.total_replica_count(cid)
    log_case("恢复后不超量", total=total, target=rep.target_replica_count)
    assert total == THRESHOLD                     # 恰好目标，不超出
    assert registry.total_replicas_system_wide() == THRESHOLD
    # 再修复一次是幂等空操作
    rep2 = RepairManager(registry).repair(cid)
    assert rep2.target_nodes == ()
    assert registry.total_replica_count(cid) == THRESHOLD


def test_online_shortage_is_filled_but_capped_when_node_flaps_back(registry):
    """在线不足确需补一份，但只补到目标：随后离线节点恢复也不超量。"""
    cid = _setup(registry)
    registry.place_replica(cid, "n0")             # 只有 1 份
    rm = RepairManager(registry)

    # 先补到目标（n1 在线被选中）
    rep1 = rm.repair(cid)
    assert rep1.target_nodes == ("n1",)

    # n1 修复窗口内离线：有效（含离线）仍有 2 份，不再补
    registry.set_node_status("n1", NodeStatus.OFFLINE)
    rep2 = rm.repair(cid)
    log_case("抖动期不补", targets=rep2.target_nodes, healthy=rep2.healthy_replica_count)
    assert rep2.target_nodes == ()

    # n1 恢复：仍恰好目标
    registry.set_node_status("n1", NodeStatus.ONLINE)
    assert registry.total_replica_count(cid) == THRESHOLD


# ---- 坏副本 + 离线好副本混合 ----

def test_corrupt_replaced_while_other_healthy_offline_no_overfill(registry):
    cid = _setup(registry)
    size = registry.get_metadata(cid).size
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")
    registry.get_node("n0").overwrite_data(cid, b"tampered")
    registry.retrieve_content(cid)                # n0 登记损坏（n1 健康在线兜底）
    assert registry.corrupt_nodes_for(cid) == ("n0",)

    # 此时把另一个好副本 n1 置离线，再修复
    registry.set_node_status("n1", NodeStatus.OFFLINE)
    rep = RepairManager(registry).repair(cid)
    log_case(
        "坏副本+离线好副本",
        replaced=rep.replaced_corrupt_nodes,
        targets=rep.target_nodes,
        healthy=rep.healthy_replica_count,
        offline_valid=rep.offline_valid_nodes,
        reason=rep.reason,
    )
    # 坏副本 n0 必须被撤下（无论是否补新的）
    assert rep.replaced_corrupt_nodes == ("n0",)
    assert not registry.get_node("n0").has(cid)
    # 此刻唯一的好副本 n1 又离线：属于"暂缓"，不动 n1 那份好副本，也不超量
    assert rep.target_nodes == ()
    assert rep.offline_valid_nodes == ("n1",)

    # n1 恢复后在线健康仍只有 1（坏副本已撤），再修一次补到目标，但绝不超量
    registry.set_node_status("n1", NodeStatus.ONLINE)
    rep2 = RepairManager(registry).repair(cid)
    log_case("恢复后续修", targets=rep2.target_nodes, healthy=rep2.healthy_replica_count)
    assert rep2.healthy_replica_count == THRESHOLD
    assert registry.total_replica_count(cid) == THRESHOLD
    assert registry.healthy_replica_count(cid) == THRESHOLD
    assert registry.corrupt_nodes_for(cid) == ()
    assert _used_bytes_consistent(registry, size)


def test_corrupt_removal_does_not_double_charge_capacity(registry):
    """原地替换：先撤坏再补一份，同一节点容量只计一次。"""
    cid = _setup(registry)
    size = registry.get_metadata(cid).size
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")
    registry.get_node("n0").overwrite_data(cid, b"tampered")
    registry.retrieve_content(cid)

    node = registry.get_node("n0")
    before_used = node.used_bytes
    rep = RepairManager(registry).repair(cid)
    log_case(
        "原地替换记账",
        replaced=rep.replaced_corrupt_nodes,
        before_used=before_used,
        after_used=node.used_bytes,
    )
    assert rep.replaced_corrupt_nodes == ("n0",)
    assert "n0" in rep.valid_nodes                # n0 原地被重新选中
    assert node.used_bytes == before_used == size  # 不重复计费
    assert node.replica_count() == 1
    assert sha256_hex(node.read(cid)) == cid
    assert _used_bytes_consistent(registry, size)


def test_all_corrupt_removed_then_rebuild_from_trusted_sample(registry):
    cid = _setup(registry)
    size = registry.get_metadata(cid).size
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")
    registry.get_node("n0").overwrite_data(cid, b"bad0")
    registry.get_node("n1").overwrite_data(cid, b"bad1")
    with pytest.raises(Exception):
        registry.retrieve_content(cid)
    assert set(registry.corrupt_nodes_for(cid)) == {"n0", "n1"}

    rep = RepairManager(registry).repair(cid)
    log_case(
        "全坏从可信样本恢复",
        targets=rep.target_nodes,
        replaced=rep.replaced_corrupt_nodes,
        healthy=rep.healthy_replica_count,
    )
    assert rep.healthy_replica_count == THRESHOLD
    assert rep.repaired
    assert len(rep.valid_nodes) == THRESHOLD
    for nid in rep.valid_nodes:
        assert sha256_hex(registry.get_node(nid).read(cid)) == cid
    assert registry.total_replica_count(cid) == THRESHOLD
    assert _used_bytes_consistent(registry, size)


# ---- 并发：反复上下线 × 修复 × 读取，多次运行稳定 ----

@pytest.mark.parametrize("round_id", range(5))
def test_flapping_nodes_with_concurrent_repairs_never_exceeds(registry, round_id):
    cid = _setup(registry, n=5)
    size = registry.get_metadata(cid).size
    # 初始恰好目标份数；抖动与修复反复交叉后有效副本数仍不得超过目标
    for nid in ("n0", "n1"):
        registry.place_replica(cid, nid)
    rm = RepairManager(registry)
    stop = threading.Event()

    def flap():
        import itertools

        for i in itertools.cycle(range(5)):
            if stop.is_set():
                return
            try:
                registry.set_node_status(f"n{i}", NodeStatus.OFFLINE)
                registry.set_node_status(f"n{i}", NodeStatus.ONLINE)
            except Exception:
                return

    def repairer():
        for _ in range(100):
            rm.repair(cid)

    def checker():
        for _ in range(300):
            # 修复类操作执行后，有效副本数永远不超过目标份数
            _assert_invariants(registry, cid, size, cap_total=THRESHOLD)

    t = threading.Thread(target=flap, daemon=True)
    t.start()
    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(checker) for _ in range(3)]
            list(pool.map(lambda _: repairer(), range(3)))
            for f in futs:
                f.result()
    finally:
        stop.set()
        t.join(timeout=5)

    for i in range(5):
        registry.set_node_status(f"n{i}", NodeStatus.ONLINE)
    rep = rm.repair(cid)
    res = registry.check_retrievability(cid)
    log_case(
        f"抖动修复并发#{round_id}",
        total=res.total_replica_count,
        healthy=res.healthy_replica_count,
        online=res.online_replica_count,
        corrupt=res.corrupt_replica_count,
        valid=rep.valid_nodes,
    )
    assert res.corrupt_replica_count == 0
    assert res.healthy_replica_count == THRESHOLD   # 恰好目标，不超
    assert res.total_replica_count == THRESHOLD
    assert res.retrievable
    assert _used_bytes_consistent(registry, size)


@pytest.mark.parametrize("round_id", range(5))
def test_flap_with_corruption_and_reads_stays_correct(registry, round_id):
    """坏副本 + 抖动 + 读取 + 修复交叉：坏数据绝不返回、好副本不丢失、不超量。"""
    cid = _setup(registry, n=4)
    size = registry.get_metadata(cid).size
    registry.place_replica(cid, "n0")
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.get_node("n0").overwrite_data(cid, b"tampered")
    registry.retrieve_content(cid)                # n0 登记损坏
    rm = RepairManager(registry)
    stop = threading.Event()

    def flap():
        import itertools

        for i in itertools.cycle(("n1", "n2", "n3")):
            if stop.is_set():
                return
            try:
                registry.set_node_status(i, NodeStatus.OFFLINE)
                registry.set_node_status(i, NodeStatus.ONLINE)
            except Exception:
                return

    def reader():
        for _ in range(200):
            try:
                data = registry.retrieve_content(cid)
                assert data == DATA               # 坏副本绝不会被当成结果
                assert sha256_hex(data) == cid
            except Exception as exc:
                # 只允许"暂时无健康在线副本"
                exc_name = type(exc).__name__
                assert exc_name == "ContentUnavailableError", exc_name

    def repairer():
        for _ in range(60):
            rm.repair(cid)

    def checker():
        for _ in range(300):
            # 初始 3 份（n0 坏），修复撤坏后为 2，因此总量上界按 3 校验；
            # 有效（未损坏）副本数任何时刻都不超过目标
            with registry.lock():
                assert registry.healthy_replica_count(cid) <= THRESHOLD
                assert registry.total_replica_count(cid) <= 3
                for node in registry.list_nodes():
                    assert node.used_bytes == node.replica_count() * size
                    assert node.replica_count() <= 1

    t = threading.Thread(target=flap, daemon=True)
    t.start()
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = [reader] * 3 + [repairer] * 3 + [checker] * 2
            list(pool.map(lambda f: f(), jobs))
    finally:
        stop.set()
        t.join(timeout=5)

    for i in range(4):
        registry.set_node_status(f"n{i}", NodeStatus.ONLINE)
    rep = rm.repair(cid)
    res = registry.check_retrievability(cid)
    log_case(
        f"坏副本抖动读取修复#{round_id}",
        online=res.online_replica_count,
        healthy=res.healthy_replica_count,
        corrupt=res.corrupt_replica_count,
        valid=rep.valid_nodes,
        replaced=rep.replaced_corrupt_nodes,
    )
    assert res.corrupt_replica_count == 0
    assert res.healthy_replica_count == THRESHOLD
    assert res.total_replica_count == THRESHOLD
    for nid in rep.valid_nodes:
        assert sha256_hex(registry.get_node(nid).read(cid)) == cid
    assert registry.retrieve_content(cid) == DATA
    assert _used_bytes_consistent(registry, size)
