"""读取链路与坏副本规避/修复：字节一致性、损坏判定、替换修复、并发交叉。"""

from __future__ import annotations

import base64
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from storage_gateway.api import create_app
from storage_gateway.errors import ContentNotFoundError, ContentUnavailableError
from storage_gateway.gateway import StorageGateway
from storage_gateway.hashing import sha256_hex
from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case

DATA = b"retrievable-content-block"


def _registry_with_replicas(registry, node_ids=("n1", "n2", "n3"), data=DATA):
    """注册节点并把内容放到前两个节点上，返回 (cid, 副本节点)。"""
    for nid in node_ids:
        registry.register_node(StorageNode(nid, 10_000))
    cid = registry.submit_content(data).cid
    holders = node_ids[:2]
    for nid in holders:
        registry.place_replica(cid, nid)
    return cid, holders


def _corrupt(registry, node_id, cid, bad=b"tampered-on-disk-bytes"):
    """模拟落盘数据被篡改：节点上的副本字节不再匹配其内容标识。"""
    assert sha256_hex(bad) != cid
    registry.get_node(node_id).overwrite_data(cid, bad)


# ---- 读取链路 ----

def test_retrieve_returns_exact_submitted_bytes_and_is_stable(registry):
    cid, _ = _registry_with_replicas(registry)
    first = registry.retrieve_content(cid)
    second = registry.retrieve_content(cid)
    log_case("读取一致性", size=len(first), stable=first == second)
    assert first == DATA
    assert second == DATA


def test_retrieve_unknown_cid_is_not_found(registry):
    log_case("从未提交", cid="0" * 16)
    with pytest.raises(ContentNotFoundError):
        registry.retrieve_content("0" * 64)


def test_retrieve_recorded_but_no_online_replica_is_unavailable(registry):
    cid, _ = _registry_with_replicas(registry, node_ids=("n1", "n2"))
    registry.set_node_status("n1", NodeStatus.OFFLINE)
    registry.set_node_status("n2", NodeStatus.OFFLINE)
    log_case("有记录无在线副本", cid=cid[:12])
    with pytest.raises(ContentUnavailableError) as exc:
        registry.retrieve_content(cid)
    # 与"从未提交"是不同原因
    assert not isinstance(exc.value, ContentNotFoundError)


def test_corrupt_replica_skipped_and_marked(registry):
    cid, _ = _registry_with_replicas(registry)
    _corrupt(registry, "n1", cid)

    data = registry.retrieve_content(cid)
    log_case("坏副本规避", got=data == DATA, corrupt=registry.corrupt_nodes_for(cid))
    assert data == DATA                       # 从健康副本取回，不是坏字节
    assert registry.corrupt_nodes_for(cid) == ("n1",)

    # 损坏副本不再计入可检索性判定
    res = registry.check_retrievability(cid)
    log_case(
        "判定排除坏副本",
        online=res.online_replica_count,
        corrupt=res.corrupt_replica_count,
        reasons=res.reasons,
    )
    assert res.online_replica_count == 1
    assert res.total_replica_count == 2      # 坏副本仍占空间，计入总数
    assert res.corrupt_replica_count == 1
    assert not res.retrievable               # 阈值 2，健康在线仅 1
    assert any("损坏" in r for r in res.reasons)


def test_all_online_replicas_corrupt_fails_explicitly(registry):
    cid, _ = _registry_with_replicas(registry, node_ids=("n1", "n2"))
    _corrupt(registry, "n1", cid)
    _corrupt(registry, "n2", cid, bad=b"other-tampered-bytes")
    log_case("全部在线副本损坏", cid=cid[:12])
    with pytest.raises(ContentUnavailableError):
        registry.retrieve_content(cid)
    assert set(registry.corrupt_nodes_for(cid)) == {"n1", "n2"}


def test_retrieve_never_returns_bytes_mismatching_cid(registry):
    cid, _ = _registry_with_replicas(registry, node_ids=("n1",))
    _corrupt(registry, "n1", cid, bad=b"x" * len(DATA))  # 等长篡改
    try:
        data = registry.retrieve_content(cid)
    except ContentUnavailableError:
        return
    assert sha256_hex(data) == cid


# ---- 坏副本修复 ----

def test_repair_replaces_corrupt_replica_in_place(registry):
    cid, _ = _registry_with_replicas(registry)
    size = registry.get_metadata(cid).size
    _corrupt(registry, "n1", cid)
    registry.retrieve_content(cid)           # 触发损坏判定

    rep = RepairManager(registry).repair(cid)
    log_case(
        "替换坏副本",
        repaired=rep.repaired,
        replaced=rep.replaced_corrupt_nodes,
        valid=rep.valid_nodes,
        after=rep.after_online_count,
    )
    assert rep.repaired
    assert rep.replaced_corrupt_nodes == ("n1",)
    # n1 上的坏副本被健康副本替换（n1 在线且容量足够，重新被选中）
    assert set(rep.valid_nodes) == {"n1", "n2"}
    assert rep.after_online_count == 2 <= 2  # 不超出修复目标（阈值）
    assert registry.total_replica_count(cid) == 2
    # 容量记账：替换不重复计费，n1 仍只占用一份
    assert registry.get_node("n1").used_bytes == size
    # 替换后的副本字节正确
    assert sha256_hex(registry.get_node("n1").read(cid)) == cid
    # 修复后读取恢复稳定
    assert registry.retrieve_content(cid) == DATA


def test_repair_never_uses_corrupt_replica_as_source(registry):
    cid, _ = _registry_with_replicas(registry)
    _corrupt(registry, "n1", cid)
    registry.retrieve_content(cid)
    rep = RepairManager(registry).repair(cid)
    assert rep.repaired
    # 修复后系统内每一份副本的字节都必须与 CID 匹配
    for nid in rep.valid_nodes:
        assert sha256_hex(registry.get_node(nid).read(cid)) == cid


def test_repair_all_corrupt_restores_from_trusted_sample(registry):
    cid, _ = _registry_with_replicas(registry, node_ids=("n1", "n2", "n3"))
    _corrupt(registry, "n1", cid)
    _corrupt(registry, "n2", cid, bad=b"n2-tampered")
    with pytest.raises(ContentUnavailableError):
        registry.retrieve_content(cid)       # 读取失败，但两个副本均被判定损坏

    rep = RepairManager(registry).repair(cid)
    log_case("全坏修复", repaired=rep.repaired, valid=rep.valid_nodes, reason=rep.reason)
    assert rep.repaired
    assert rep.after_online_count == 2
    assert set(rep.replaced_corrupt_nodes) == {"n1", "n2"}
    for nid in rep.valid_nodes:
        assert sha256_hex(registry.get_node(nid).read(cid)) == cid


def test_resubmit_after_repair_creates_no_extra_replicas(registry):
    cid, _ = _registry_with_replicas(registry)
    _corrupt(registry, "n1", cid)
    registry.retrieve_content(cid)
    RepairManager(registry).repair(cid)
    before = registry.total_replica_count(cid)

    meta = registry.submit_content(DATA)     # 原内容再次提交
    log_case("修复后再提交", cid_same=meta.cid == cid, total=registry.total_replica_count(cid))
    assert meta.cid == cid
    assert registry.total_replica_count(cid) == before
    assert registry.total_replicas_system_wide() == before


# ---- HTTP 读取 ----

def test_http_retrieve_roundtrip_and_error_distinction():
    client = TestClient(create_app(StorageGateway(retrieval_threshold=2)))
    for nid in ("n1", "n2"):
        client.post("/nodes", json={"node_id": nid, "capacity": 10_000})
    r = client.post("/contents", json={"content_b64": base64.b64encode(DATA).decode()})
    cid = r.json()["cid"]
    client.post(f"/contents/{cid}/replicas", json={"node_ids": ["n1", "n2"]})

    r = client.get(f"/contents/{cid}")
    log_case("HTTP读取", status=r.status_code, size=len(r.content))
    assert r.status_code == 200
    assert r.content == DATA

    # 从未提交 -> 404；有记录但无健康副本 -> 503
    assert client.get(f"/contents/{'0' * 64}").status_code == 404
    client.post("/nodes/n1/status", json={"status": "offline"})
    client.post("/nodes/n2/status", json={"status": "offline"})
    r = client.get(f"/contents/{cid}")
    log_case("HTTP无健康副本", status=r.status_code, error=r.json()["error_type"])
    assert r.status_code == 503
    assert r.json()["error_type"] == "ContentUnavailableError"


# ---- 并发：读取与修复交叉 ----

def test_concurrent_reads_of_same_content_are_identical(registry):
    cid, _ = _registry_with_replicas(registry)
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: registry.retrieve_content(cid), range(64)))
    log_case("并发读取", distinct=len(set(results)))
    assert len(set(results)) == 1
    assert results[0] == DATA


def test_concurrent_read_and_repair_with_corruption(registry):
    """读取与修复并发交叉：坏副本绝不被当成结果，最终状态可解释且守恒。"""
    cid, _ = _registry_with_replicas(registry, node_ids=("n1", "n2", "n3", "n4"))
    _corrupt(registry, "n1", cid)
    rm = RepairManager(registry)
    barrier = threading.Barrier(12)
    outcomes: list[str] = []
    lock = threading.Lock()

    def reader():
        barrier.wait()
        try:
            data = registry.retrieve_content(cid)
            assert data == DATA, "读取返回了与提交不一致的字节"
            with lock:
                outcomes.append("ok")
        except ContentUnavailableError:
            with lock:
                outcomes.append("unavailable")

    def repairer():
        barrier.wait()
        rm.repair(cid)

    actors = [reader] * 8 + [repairer] * 4
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda f: f(), actors))

    res = registry.check_retrievability(cid)
    per_node_counts = {
        n.node_id: n.replica_count() for n in registry.list_nodes()
    }
    log_case(
        "读取修复并发",
        outcomes={k: outcomes.count(k) for k in set(outcomes)},
        online=res.online_replica_count,
        corrupt=res.corrupt_replica_count,
        per_node=per_node_counts,
    )
    # 修复结束后：健康在线副本恰好达到阈值、无残留损坏登记
    assert res.retrievable
    assert res.online_replica_count == 2
    assert res.corrupt_replica_count == 0
    # 每节点至多一份副本，系统总副本数不超过阈值
    assert all(c <= 1 for c in per_node_counts.values())
    assert registry.total_replica_count(cid) <= 2
    # 每份现存副本字节都健康，且容量记账与实际副本数一致
    size = registry.get_metadata(cid).size
    for node in registry.list_nodes():
        for held in node.replicas():
            assert sha256_hex(node.read(held)) == held
        assert node.used_bytes == node.replica_count() * size
    # 修复后读取稳定
    assert registry.retrieve_content(cid) == DATA


def test_read_and_repair_while_nodes_flap(registry):
    """节点上下线与读取/修复交叉：读取永不返回坏字节，计数始终守恒。"""
    cid, _ = _registry_with_replicas(registry, node_ids=("n1", "n2", "n3"))
    _corrupt(registry, "n1", cid)
    rm = RepairManager(registry)
    stop = threading.Event()

    def flap():
        import itertools

        for i in itertools.cycle(("n2", "n3")):
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
                assert registry.retrieve_content(cid) == DATA
            except ContentUnavailableError:
                pass

    def repairer():
        for _ in range(20):
            rm.repair(cid)

    t = threading.Thread(target=flap, daemon=True)
    t.start()
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda f: f(), [reader] * 4 + [repairer] * 2))
    stop.set()
    t.join(timeout=5)

    # 让所有节点回到在线，做最终修复并核验守恒
    for nid in ("n1", "n2", "n3"):
        registry.set_node_status(nid, NodeStatus.ONLINE)
    rm.repair(cid)
    res = registry.check_retrievability(cid)
    log_case("抖动交叉", online=res.online_replica_count, corrupt=res.corrupt_replica_count)
    assert res.retrievable
    assert res.corrupt_replica_count == 0
    assert res.online_replica_count <= res.threshold
    size = registry.get_metadata(cid).size
    for node in registry.list_nodes():
        assert node.used_bytes == node.replica_count() * size
