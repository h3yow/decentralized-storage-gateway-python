"""修复与节点上下线抖动交叉：有效副本数（含暂时离线的健康副本）不得超过目标份数。

口径定义：
- 有效副本 = 节点持有且未被判定损坏的副本，**不区分在线/离线**；
- 修复目标 = 让有效副本数达到 retrieval_threshold，且修复本身绝不把它推过阈值；
- 暂时离线的健康副本在"要不要补"的判断中必须被计入。
"""

from __future__ import annotations

from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case


def _effective_count(registry, cid: str) -> int:
    corrupt = set(registry.corrupt_nodes_for(cid))
    return sum(
        1
        for n in registry.list_nodes()
        if n.has(cid) and n.node_id not in corrupt
    )


def _force_corruption_detection(registry, cid: str) -> None:
    """逐节点隔离读取：让每个持副本节点轮流成为唯一在线候选，

    凡字节与 CID 不符的副本都会在读取校验时被登记为损坏
    （损坏判定本身是惰性的，这里用公开 API 把判定强制执行一遍）。
    """
    from storage_gateway.errors import ContentUnavailableError

    holders = [n.node_id for n in registry.list_nodes() if n.has(cid)]
    for nid in holders:
        others = [
            n.node_id for n in registry.list_nodes() if n.node_id != nid
        ]
        for other in others:
            registry.set_node_status(other, NodeStatus.OFFLINE)
        try:
            registry.retrieve_content(cid)
        except ContentUnavailableError:
            pass
        finally:
            for other in others:
                registry.set_node_status(other, NodeStatus.ONLINE)


def test_offline_healthy_replica_counts_toward_target(registry):
    """好副本节点修复期间离线：不能再补新副本，恢复在线后有效副本数不超标。"""
    for nid in ("n1", "n2", "n3"):
        registry.register_node(StorageNode(nid, 10_000))
    cid = registry.submit_content(b"flap-target").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")

    # n2 持有好副本但暂时离线 -> 在线健康副本只有 1，低于阈值 2
    registry.set_node_status("n2", NodeStatus.OFFLINE)
    assert registry.online_replica_count(cid) == 1

    rep = RepairManager(registry).repair(cid)
    log_case(
        "离线好副本计入目标",
        repaired=rep.repaired,
        targets=rep.target_nodes,
        reason=rep.reason,
        valid=rep.valid_nodes,
    )
    # 有效副本（n1 + 暂时离线的 n2）已达目标份数：一份都不许补
    assert rep.target_nodes == ()
    assert registry.total_replica_count(cid) == 2

    # n2 恢复上线：有效副本数恰好等于目标份数，不超量
    registry.set_node_status("n2", NodeStatus.ONLINE)
    assert _effective_count(registry, cid) == 2
    assert registry.online_replica_count(cid) == 2


def test_repair_report_explains_effective_count_and_valid_nodes(registry):
    """修复报告必须说清：有效副本在哪些节点、坏副本是否被换掉、最终未超标。"""
    for nid in ("n1", "n2", "n3"):
        registry.register_node(StorageNode(nid, 10_000))
    cid = registry.submit_content(b"report").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    # n1 副本被篡改并判定损坏（按节点 id 排序 n1 先被读取校验），随后 n1 离线
    registry.get_node("n1").overwrite_data(cid, b"tampered")
    registry.retrieve_content(cid)
    registry.set_node_status("n1", NodeStatus.OFFLINE)

    rep = RepairManager(registry).repair(cid)
    log_case(
        "报告口径",
        valid=rep.valid_nodes,
        replaced=rep.replaced_corrupt_nodes,
        effective=rep.after_effective_count,
        target=rep.target_count,
        reason=rep.reason,
    )
    # 坏副本被移除（n1 离线也可移除），有效副本数不足目标 -> 补到 n3
    assert rep.replaced_corrupt_nodes == ("n1",)
    assert set(rep.valid_nodes) == {"n2", "n3"}
    assert rep.after_effective_count == 2
    assert rep.target_count == 2
    assert rep.after_effective_count <= rep.target_count
    # n1 上的坏副本确实被换掉：节点不再持有该内容，容量记账归零
    assert not registry.get_node("n1").has(cid)
    assert registry.get_node("n1").used_bytes == 0
    # n1 恢复上线后有效副本数仍不超标
    registry.set_node_status("n1", NodeStatus.ONLINE)
    assert _effective_count(registry, cid) == 2


def test_replace_corrupt_on_flapping_node_keeps_capacity_consistent(registry):
    """坏副本节点在修复期间上下线：先撤后补不会把同一节点容量算重。"""
    for nid in ("n1", "n2"):
        registry.register_node(StorageNode(nid, 100))
    cid = registry.submit_content(b"x" * 50).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    # n1 按节点 id 排序先被读取：篡改它才能在读取时被判定损坏
    registry.get_node("n1").overwrite_data(cid, b"y" * 50)
    registry.retrieve_content(cid)  # 判定 n1 损坏
    assert registry.get_node("n1").used_bytes == 50

    rep = RepairManager(registry).repair(cid)
    log_case(
        "原地替换记账",
        replaced=rep.replaced_corrupt_nodes,
        valid=rep.valid_nodes,
        n1_used=registry.get_node("n1").used_bytes,
    )
    assert rep.replaced_corrupt_nodes == ("n1",)
    # 先撤（-50）后补（+50）：n1 仍只计一份，不会算成 100
    assert registry.get_node("n1").used_bytes == 50
    assert registry.get_node("n1").replica_count() == 1
    assert _effective_count(registry, cid) == 2


def test_repair_under_repeated_flapping_never_exceeds_target(registry):
    """确定性多轮：每轮随机选一批好副本节点离线后修复，有效副本数永不超标。"""
    import random

    for nid in ("n1", "n2", "n3", "n4"):
        registry.register_node(StorageNode(nid, 10_000))
    cid = registry.submit_content(b"rounds").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    rm = RepairManager(registry)
    rng = random.Random(20261007)  # 固定种子：失败可复现

    for round_no in range(50):
        holders = [
            n.node_id for n in registry.list_nodes() if n.has(cid)
        ]
        # 随机让部分持副本节点离线（至少保留一个在线健康样本）
        offlined = [h for h in holders if rng.random() < 0.5]
        if len(offlined) == len(holders):
            offlined = offlined[1:]
        for nid in offlined:
            registry.set_node_status(nid, NodeStatus.OFFLINE)

        rep = rm.repair(cid)
        effective_mid = _effective_count(registry, cid)
        assert effective_mid <= 2, (
            f"第 {round_no} 轮修复中超标：effective={effective_mid}, "
            f"offlined={offlined}, reason={rep.reason}"
        )
        assert rep.after_effective_count <= rep.target_count

        for nid in offlined:
            registry.set_node_status(nid, NodeStatus.ONLINE)
        effective_after = _effective_count(registry, cid)
        assert effective_after <= 2, (
            f"第 {round_no} 轮节点恢复后超标：effective={effective_after}"
        )
    log_case(
        "多轮抖动修复",
        rounds=50,
        final_effective=_effective_count(registry, cid),
        final_online=registry.online_replica_count(cid),
    )
    assert registry.total_replica_count(cid) == 2


def test_concurrent_repair_read_and_flapping_stays_consistent(registry):
    """多线程压力：修复、读取、上下线抖动交叉，坏副本绝不返回、计数永不超标。"""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from storage_gateway.errors import ContentUnavailableError
    from storage_gateway.hashing import sha256_hex

    data = b"stress-block"
    for nid in ("n1", "n2", "n3", "n4"):
        registry.register_node(StorageNode(nid, 10_000))
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.get_node("n2").overwrite_data(cid, b"corrupted")  # 待判定坏副本
    rm = RepairManager(registry)
    stop = threading.Event()
    read_errors: list[AssertionError] = []

    def flap():
        import itertools

        for nid in itertools.cycle(("n1", "n2", "n3")):
            if stop.is_set():
                return
            registry.set_node_status(nid, NodeStatus.OFFLINE)
            registry.set_node_status(nid, NodeStatus.ONLINE)

    def reader():
        for _ in range(150):
            try:
                got = registry.retrieve_content(cid)
                if got != data:  # 坏副本被当成好副本返回：记录而非中断线程
                    read_errors.append(AssertionError(f"bad bytes: {got!r}"))
            except ContentUnavailableError:
                pass

    def repairer():
        for _ in range(30):
            rm.repair(cid)

    t = threading.Thread(target=flap, daemon=True)
    t.start()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda f: f(), [reader] * 4 + [repairer] * 4))
    stop.set()
    t.join(timeout=5)

    assert not read_errors, f"读取返回了坏字节: {read_errors[:3]}"

    # 全部节点恢复在线。损坏判定是惰性的（读取校验失败才登记），
    # 可能仍有"实际已坏但尚未被读取发现"的副本：先逐节点隔离读取强制检测，
    # 再走最终修复把它们清掉。
    for nid in ("n1", "n2", "n3", "n4"):
        registry.set_node_status(nid, NodeStatus.ONLINE)
    _force_corruption_detection(registry, cid)
    rep = rm.repair(cid)
    res = registry.check_retrievability(cid)
    size = registry.get_metadata(cid).size
    per_node = {n.node_id: n.replica_count() for n in registry.list_nodes()}
    log_case(
        "压力交叉",
        effective=rep.after_effective_count,
        target=rep.target_count,
        online=res.online_replica_count,
        corrupt=res.corrupt_replica_count,
        per_node=per_node,
    )
    # 有效副本数不超标、无残留损坏登记、每节点至多一份
    assert _effective_count(registry, cid) <= 2
    assert rep.after_effective_count <= rep.target_count
    assert res.corrupt_replica_count == 0
    assert all(c <= 1 for c in per_node.values())
    # 每份现存副本字节健康，容量记账与实际副本数一致
    for node in registry.list_nodes():
        for held in node.replicas():
            assert sha256_hex(node.read(held)) == held
        assert node.used_bytes == node.replica_count() * size
    # 修复后读取稳定
    assert registry.retrieve_content(cid) == data
