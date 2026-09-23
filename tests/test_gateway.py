"""网关单元测试：覆盖损坏、容量、授权、上下线、并发修复与守恒断言。

日志约定（caplog 中可查到）：
- 每个入口打印 input（提交内容、节点、期望校验值等）；
- 每次判定打印 decision basis（在线副本数、阈值、失败原因）。
"""

from __future__ import annotations

import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from gateway import (
    ContentStatus,
    FailureReason,
    GatewayConfig,
    GatewayError,
    LocalStorageNode,
    StorageGateway,
)


@pytest.fixture
def gw():
    g = StorageGateway(GatewayConfig(min_replicas=2))
    for nid, cap in [("n1", 100), ("n2", 100), ("n3", 100), ("n4", 5)]:
        g.register_node(LocalStorageNode(nid, cap))
    g.register_node(LocalStorageNode("bad", 100, authorized=False))
    return g


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def assert_conservation(g: StorageGateway) -> None:
    """全局守恒：网关记录的副本数 == 节点真实存储数；used 与真实字节一致。"""
    node_real = sum(len(n._data) for n in g._nodes.values())
    logical = g.total_replicas(online_only=False)
    assert logical == node_real, f"logical replicas {logical} != nodes hold {node_real}"
    for node in g._nodes.values():
        assert node.info.used == sum(len(v) for v in node._data.values())
        assert node.info.used <= node.info.capacity


# ---------- 内容寻址：去重 ----------

def test_submit_duplicate_content_dedup(gw, caplog):
    caplog.set_level(logging.INFO)
    cid1 = gw.submit(b"hello")
    cid2 = gw.submit(b"hello")
    assert cid1 == cid2 == sha256(b"hello")
    # 重复提交不产生任何副本，也不产生新内容记录
    assert gw.total_replicas() == 0
    assert any("deduped" in r.message for r in caplog.records)
    assert any("input" in r.message for r in caplog.records)


def test_submit_checksum_mismatch_rejected(gw, caplog):
    caplog.set_level(logging.WARNING)
    with pytest.raises(GatewayError) as ei:
        gw.submit(b"hello", expected_checksum="deadbeef")
    assert ei.value.reason is FailureReason.CHECKSUM_MISMATCH
    assert gw.total_replicas() == 0
    assert any(FailureReason.CHECKSUM_MISMATCH.value in r.message for r in caplog.records)


# ---------- 副本放置：容量、授权、重复 ----------

def test_place_replica_basic_and_duplicate(gw):
    cid = gw.submit(b"payload-123")
    gw.place_replica(cid, "n1")
    with pytest.raises(GatewayError) as ei:
        gw.place_replica(cid, "n1")
    assert ei.value.reason is FailureReason.DUPLICATE_PLACEMENT
    assert gw.replica_count(cid, online_only=False) == 1
    assert_conservation(gw)


def test_place_replica_capacity_exceeded(gw, caplog):
    cid = gw.submit(b"x" * 50)  # n4 容量只有 5
    with pytest.raises(GatewayError) as ei:
        gw.place_replica(cid, "n4")
    assert ei.value.reason is FailureReason.CAPACITY_EXCEEDED
    assert not gw._nodes["n4"].has(cid)
    assert_conservation(gw)


def test_place_replica_unauthorized(gw):
    cid = gw.submit(b"secret")
    with pytest.raises(GatewayError) as ei:
        gw.place_replica(cid, "bad")
    assert ei.value.reason is FailureReason.UNAUTHORIZED_NODE
    assert_conservation(gw)


def test_place_replica_offline_node(gw):
    cid = gw.submit(b"offline-block")
    gw.set_node_status("n3", online=False)
    with pytest.raises(GatewayError) as ei:
        gw.place_replica(cid, "n3")
    assert ei.value.reason is FailureReason.NODE_OFFLINE
    assert_conservation(gw)


# ---------- 损坏检测 ----------

def test_corrupted_stored_bytes_detected_on_get(gw):
    cid = gw.submit(b"integrity")
    gw.place_replica(cid, "n1")
    gw.place_replica(cid, "n2")
    gw.simulate_corruption("n1", cid, b"tampered!!!")
    with pytest.raises(GatewayError) as ei:
        gw.get(cid)
    assert ei.value.reason is FailureReason.CONTENT_CORRUPTED
    assert_conservation(gw)


def test_corrupted_source_skipped_during_repair(gw, caplog):
    caplog.set_level(logging.INFO)
    cid = gw.submit(b"repair-me")
    gw.place_replica(cid, "n1")  # 唯一在线完好样本
    gw.place_replica(cid, "n2")
    gw.simulate_corruption("n2", cid, b"bad-data")
    gw.set_node_status("n1", online=False)  # 好样本下线
    # 只剩损坏副本在线：没有可用样本源
    with pytest.raises(GatewayError) as ei:
        gw.repair(cid)
    assert ei.value.reason is FailureReason.NO_REPAIR_SOURCE
    # 修复失败保留原状态与原副本
    assert gw.retrievability(cid) is ContentStatus.UNRETRIEVABLE
    assert set(gw._record(cid).replicas) == {"n1", "n2"}
    assert_conservation(gw)


# ---------- 节点上下线 ----------

def test_node_on_offline_updates_retrievability(gw, caplog):
    caplog.set_level(logging.INFO)
    cid = gw.submit(b"availability")
    gw.place_replica(cid, "n1")
    gw.place_replica(cid, "n2")
    assert gw.retrievability(cid) is ContentStatus.RETRIEVABLE

    gw.set_node_status("n1", online=False)
    assert gw.replica_count(cid) == 1  # 在线副本
    assert gw.replica_count(cid, online_only=False) == 2  # 逻辑副本不丢
    assert gw.retrievability(cid) is ContentStatus.UNRETRIEVABLE

    gw.set_node_status("n1", online=True)
    assert gw.retrievability(cid) is ContentStatus.RETRIEVABLE
    # 判定依据（在线数与阈值）必须出现在日志里
    assert any("online_replicas=1 min=2" in r.message for r in caplog.records)
    assert_conservation(gw)


# ---------- 修复：成功、失败保留原因 ----------

def test_repair_restores_threshold(gw):
    cid = gw.submit(b"need-repair")
    gw.place_replica(cid, "n1")
    gw.place_replica(cid, "n2")
    gw.set_node_status("n1", online=False)
    assert gw.retrievability(cid) is ContentStatus.UNRETRIEVABLE

    status = gw.repair(cid)
    assert status is ContentStatus.RETRIEVABLE
    # 只补到阈值，不多复制；原有离线副本仍在
    assert gw.replica_count(cid) == 2
    assert gw.replica_count(cid, online_only=False) == 3
    assert "n1" in gw._record(cid).replicas
    assert_conservation(gw)


def test_repair_failure_keeps_state_and_reports_reason(gw):
    cid = gw.submit(b"z" * 20)
    gw.place_replica(cid, "n1")
    gw.place_replica(cid, "n2")
    gw.set_node_status("n2", online=False)
    # n3 离线、n4 容量不够、bad 未授权：无法补到阈值
    gw.set_node_status("n3", online=False)
    before = gw.replica_count(cid, online_only=False)
    with pytest.raises(GatewayError) as ei:
        gw.repair(cid)
    assert ei.value.reason is FailureReason.INSUFFICIENT_REPLICAS
    assert "capacity_exceeded" in ei.value.detail
    assert gw.retrievability(cid) is ContentStatus.UNRETRIEVABLE
    assert gw.replica_count(cid, online_only=False) == before
    assert_conservation(gw)


# ---------- 并发：修复不重复、不丢失；去重不冲突 ----------

def test_concurrent_repairs_do_not_duplicate_or_lose(gw):
    cid = gw.submit(b"race-content")
    gw.place_replica(cid, "n1")  # 唯一样本

    errors: list[GatewayError] = []

    def repair_once():
        try:
            gw.repair(cid)
        except GatewayError as exc:  # 其余线程可能看到“已达标”，属于幂等
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: repair_once(), range(16)))

    assert errors == []
    # 唯一样本还在；总数恰好补到阈值，没有重复复制
    assert "n1" in gw._record(cid).replicas
    assert gw.replica_count(cid) == gw.config.min_replicas
    assert gw.replica_count(cid, online_only=False) == gw.config.min_replicas
    assert gw.retrievability(cid) is ContentStatus.RETRIEVABLE
    assert_conservation(gw)


def test_concurrent_repair_and_dedup_submit(gw):
    data = b"dedup-under-repair"

    cid_first = gw.submit(data)
    gw.place_replica(cid_first, "n1")

    submitted: list[str] = []
    workers = 9
    barrier = threading.Barrier(workers)

    def worker():
        barrier.wait()
        submitted.append(gw.submit(data))  # 重复提交
        gw.repair(cid_first)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(lambda _: worker(), range(workers)))

    assert set(submitted) == {cid_first}
    assert gw.replica_count(cid_first) == gw.config.min_replicas
    assert_conservation(gw)


def test_concurrent_placement_counter_consistency(gw):
    cid = gw.submit(b"counter")
    nodes = ["n1", "n2", "n3"]
    barrier = threading.Barrier(len(nodes))

    def place(nid):
        barrier.wait()
        gw.place_replica(cid, nid)

    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(place, nodes))

    assert gw.replica_count(cid, online_only=False) == 3
    assert sum(n.info.used for n in gw._nodes.values()) == 3 * len(b"counter")
    assert_conservation(gw)
