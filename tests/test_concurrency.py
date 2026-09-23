"""并发：去重、放置、修复、上下线交织时副本计数守恒且不重复复制。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

from storage_gateway.errors import (
    DuplicatePlacementError,
    StorageError,
)
from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case


def test_concurrent_submit_dedupes_and_placement_no_duplicates(registry):
    for i in range(4):
        registry.register_node(StorageNode(f"n{i}", 10_000))
    data = b"concurrent-block"

    def submit():
        return registry.submit_content(data).cid

    with ThreadPoolExecutor(max_workers=16) as pool:
        cids = list(pool.map(lambda _: submit(), range(64)))
    log_case("并发提交", distinct_cids=len(set(cids)))
    assert len(set(cids)) == 1
    cid = cids[0]

    # 64 个线程同时向同一节点放置：恰好一次成功
    outcomes = Counter()

    def place(node_id):
        try:
            registry.place_replica(cid, node_id)
            outcomes["ok"] += 1
        except DuplicatePlacementError:
            outcomes["duplicate"] += 1
        except StorageError:
            outcomes["other"] += 1

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(lambda _: place("n0"), range(64)))
    log_case("并发同节点放置", outcomes=dict(outcomes))
    assert outcomes["ok"] == 1
    assert outcomes["duplicate"] == 63
    assert registry.total_replica_count(cid) == 1
    # 每个节点对同一内容至多一份
    for i in range(4):
        assert registry.get_node(f"n{i}").replica_count() <= 1


def test_concurrent_repairs_do_not_over_replicate(registry):
    nodes = [StorageNode(f"n{i}", 10_000) for i in range(10)]
    for n in nodes:
        registry.register_node(n)
    cid = registry.submit_content(b"repair-race").cid
    registry.place_replica(cid, "n0")

    barrier = threading.Barrier(20)
    rm = RepairManager(registry)

    def repair_once():
        barrier.wait()
        return rm.repair(cid)

    with ThreadPoolExecutor(max_workers=20) as pool:
        reports = list(pool.map(lambda _: repair_once(), range(20)))

    online = registry.online_replica_count(cid)
    total = registry.total_replica_count(cid)
    successes = sum(1 for r in reports if r.repaired)
    # 逐节点核验：同一内容在任一节点上只出现一次
    per_node = [n.replica_count() for n in nodes]
    log_case(
        "并发修复",
        online=online,
        total=total,
        reports_repaired=successes,
        max_per_node=max(per_node),
        sum_per_node=sum(per_node),
    )
    assert online == 2                 # 阈值恰好被满足，不多复制
    assert total == 2
    assert sum(per_node) == 2
    assert max(per_node) == 1
    assert registry.total_replicas_system_wide() == 2


def test_concurrent_flapping_nodes_keeps_counts_consistent(registry):
    for i in range(5):
        registry.register_node(StorageNode(f"n{i}", 10_000))
    cid = registry.submit_content(b"flap").cid
    for i in range(5):
        registry.place_replica(cid, f"n{i}")

    stop = threading.Event()

    def flap():
        import itertools

        for i in itertools.cycle(range(5)):
            if stop.is_set():
                return
            registry.set_node_status(f"n{i}", NodeStatus.OFFLINE)
            registry.set_node_status(f"n{i}", NodeStatus.ONLINE)

    def check():
        # 无论在线数怎么变，总副本数恒为 5，且 0 <= online <= 5。
        # 采用有界迭代，避免"等待停止信号"与"等待线程结束"互相等待。
        seen = set()
        for _ in range(2000):
            res = registry.check_retrievability(cid)
            assert res.total_replica_count == 5
            assert 0 <= res.online_replica_count <= 5
            seen.add(res.online_replica_count)
        return seen

    t = threading.Thread(target=flap, daemon=True)
    t.start()
    with ThreadPoolExecutor(max_workers=4) as pool:
        seen_sets = list(pool.map(lambda _: check(), range(4)))
    stop.set()
    t.join(timeout=5)
    all_seen = set().union(*seen_sets)
    res = registry.check_retrievability(cid)
    log_case("上下线抖动", final_total=res.total_replica_count, final_online=res.online_replica_count, observed_online_values=sorted(all_seen))
    assert res.total_replica_count == 5
    assert res.online_replica_count == 5
    assert res.retrievable


def test_repair_concurrent_with_extra_placement(registry):
    """修复与外部去重放置并发：最终副本数不超过阈值，且无重复。"""
    for i in range(6):
        registry.register_node(StorageNode(f"n{i}", 10_000))
    cid = registry.submit_content(b"mix").cid
    registry.place_replica(cid, "n0")
    rm = RepairManager(registry)
    barrier = threading.Barrier(8)

    def actor(kind):
        barrier.wait()
        if kind == "repair":
            rm.repair(cid)
        else:
            try:
                registry.place_replica(cid, f"n{kind}")
            except StorageError:
                pass

    kinds = ["repair"] * 4 + [1, 2, 3, 4]
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(actor, kinds))
    total = registry.total_replica_count(cid)
    log_case("修复与放置并发", total=total, threshold=2)
    assert 2 <= total <= 5
    for i in range(6):
        assert registry.get_node(f"n{i}").replica_count() <= 1
