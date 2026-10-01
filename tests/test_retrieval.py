"""读取链路：按 CID 取回原始字节、坏副本自动隔离与改取、失败原因可区分。"""

from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from storage_gateway.api import create_app
from storage_gateway.gateway import StorageGateway
from storage_gateway.hashing import sha256_hex
from storage_gateway.models import NodeStatus
from storage_gateway.node import StorageNode
from storage_gateway.repair import RepairManager

from .conftest import log_case


def _cluster(reg, n=3, cap=1000):
    for i in range(1, n + 1):
        reg.register_node(StorageNode(f"n{i}", cap))


def test_retrieve_returns_exact_submitted_bytes(registry):
    _cluster(registry)
    data = b"the-quick-brown-fox" * 7
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    got = registry.retrieve_content(cid)
    log_case("取回一致", cid=cid[:12], size=len(got))
    assert got == data
    assert sha256_hex(got) == cid


def test_repeated_retrievals_are_stable(registry):
    _cluster(registry)
    data = b"stable-content"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    results = {registry.retrieve_content(cid) for _ in range(10)}
    log_case("重复读取稳定", distinct=len(results))
    assert results == {data}


def test_unknown_cid_raises_not_found(registry):
    from storage_gateway.errors import ContentNotFoundError

    with pytest.raises(ContentNotFoundError):
        registry.retrieve_content(sha256_hex(b"never submitted"))


def test_no_replica_is_distinct_from_not_found(registry):
    from storage_gateway.errors import ReplicaUnavailableError

    _cluster(registry)
    cid = registry.submit_content(b"registered-but-nowhere").cid
    with pytest.raises(ReplicaUnavailableError) as exc:
        registry.retrieve_content(cid)
    log_case("有记录无副本", error=type(exc.value).__name__, msg=str(exc.value))
    assert "尚无任何副本" in str(exc.value)


def test_healthy_replica_offline_is_unavailable_not_not_found(registry):
    from storage_gateway.errors import ReplicaUnavailableError

    _cluster(registry)
    cid = registry.submit_content(b"only-copy").cid
    registry.place_replica(cid, "n1")
    registry.set_node_status("n1", NodeStatus.OFFLINE)
    with pytest.raises(ReplicaUnavailableError) as exc:
        registry.retrieve_content(cid)
    log_case("健康副本离线", msg=str(exc.value))
    assert "离线" in str(exc.value)


def test_tampered_replica_is_never_returned_and_other_healthy_used(registry):
    _cluster(registry)
    data = b"important-original"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")

    # 故障注入：n1 落盘字节被改成与 CID 不符的内容
    registry.tamper_replica(cid, "n1", b"evil-bytes-on-disk")
    got = registry.retrieve_content(cid)
    log_case("坏副本被规避", got=got, corrupt=sorted(registry.corrupted_nodes(cid)))
    assert got == data  # 绝不返回坏数据
    assert registry.corrupted_nodes(cid) == frozenset({"n1"})
    res = registry.check_retrievability(cid)
    assert res.online_replica_count == 1
    assert res.online_corrupted_count == 1


def test_all_online_replicas_corrupt_is_unavailable_with_reason(registry):
    from storage_gateway.errors import ReplicaUnavailableError

    _cluster(registry)
    cid = registry.submit_content(b"both-corrupt").cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"bad1")
    registry.tamper_replica(cid, "n2", b"bad2")
    with pytest.raises(ReplicaUnavailableError) as exc:
        registry.retrieve_content(cid)
    log_case("在线全损坏", msg=str(exc.value))
    assert "字节与 CID 不符" in str(exc.value)
    res = registry.check_retrievability(cid)
    assert not res.retrievable
    assert res.online_replica_count == 0 and res.online_corrupted_count == 2


def test_corrupt_replica_excluded_from_counts_and_health(registry):
    _cluster(registry)
    data = b"count-me"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"x")
    # 总副本占位仍是 2（坏副本未被删除），但健康/在线健康数只剩 1
    assert registry.total_replica_count(cid) == 2
    assert registry.online_replica_count(cid) == 1
    assert registry.healthy_replica_count(cid) == 1


def test_repair_replaces_corrupt_replica_without_extra_copy_or_double_charge(registry):
    _cluster(registry)
    data = b"repair-corrupt"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"corrupt-bytes")
    # 触发一次判定让 n1 被隔离
    registry.check_retrievability(cid)

    n1 = registry.get_node("n1")
    n3 = registry.get_node("n3")
    charged_before = n1.used_bytes
    system_before = registry.total_replicas_system_wide()

    rep = RepairManager(registry).repair(cid)
    log_case(
        "修复替换坏副本",
        replaced=rep.replaced_nodes,
        added=rep.target_nodes,
        valid=rep.valid_replica_count,
    )
    assert rep.repaired
    assert rep.replaced_nodes == ("n1",)
    assert rep.target_nodes == ()                 # 不凭空新增副本
    assert rep.valid_replica_count == 2          # 有效副本不超过阈值 2
    assert registry.total_replica_count(cid) == 2
    assert n1.read_replica(cid) == data          # 坏副本被正确字节替换
    assert n1.used_bytes == charged_before        # 容量记账不重算
    assert n3.replica_count() == 0                # 没占第三个节点
    assert registry.total_replicas_system_wide() == system_before
    assert registry.corrupted_nodes(cid) == frozenset()
    # 修复后重新取回必然正确
    assert registry.retrieve_content(cid) == data


def test_repair_fills_missing_after_replacing_corrupt_only_to_threshold(registry):
    # 4 节点阈值 2：n1 坏、n2 健康 -> 修复只替换 n1，不新增，避免超标
    for i in range(1, 5):
        registry.register_node(StorageNode(f"n{i}", 1000))
    data = b"to-threshold"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"bad")
    rep = RepairManager(registry).repair(cid)
    log_case("替换即达标", replaced=rep.replaced_nodes, added=rep.target_nodes, valid=rep.valid_replica_count)
    assert rep.replaced_nodes == ("n1",)
    assert rep.target_nodes == ()
    assert rep.valid_replica_count == 2


def test_repair_adds_when_healthy_count_below_threshold_then_stops(registry):
    for i in range(1, 6):
        registry.register_node(StorageNode(f"n{i}", 1000))
    data = b"need-more"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")  # 唯一健康副本
    rep = RepairManager(registry).repair(cid)
    log_case("补齐到阈值", added=rep.target_nodes, valid=rep.valid_replica_count)
    assert rep.repaired and len(rep.target_nodes) == 1
    assert rep.valid_replica_count == 2
    # 再多修一次也是空操作，不会超标
    rep2 = RepairManager(registry).repair(cid)
    assert not rep2.repaired and rep2.target_nodes == ()
    assert registry.healthy_replica_count(cid) == 2


def test_corrupt_replica_not_used_as_repair_source(registry):
    # n1 坏、n2 健康在线：修复后 n1 字节必须等于 n2（可信样本），而不是坏字节扩散
    _cluster(registry)
    data = b"source-integrity"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    bad = b"do-not-propagate"
    registry.tamper_replica(cid, "n1", bad)
    RepairManager(registry).repair(cid)
    assert registry.get_node("n1").read_replica(cid) == data
    assert registry.get_node("n1").read_replica(cid) != bad


def test_resubmit_after_repair_creates_no_extra_replicas(registry):
    _cluster(registry)
    data = b"resubmit-me"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"bad")
    RepairManager(registry).repair(cid)
    before = registry.total_replica_count(cid)
    meta = registry.submit_content(data)
    log_case("修复后重提", cid=meta.cid[:8], before=before, after=registry.total_replica_count(cid))
    assert meta.cid == cid
    assert registry.total_replica_count(cid) == before == 2


def test_http_retrieve_and_error_distinction():
    gw = StorageGateway(retrieval_threshold=2)
    client = TestClient(create_app(gw))
    reg = gw.registry
    _cluster(reg)
    data = b"http-retrieve"
    r = client.post("/contents", json={"content_b64": base64.b64encode(data).decode()})
    cid = r.json()["cid"]
    gw.replicate(cid, ["n1", "n2"])

    r = client.get(f"/contents/{cid}/bytes")
    log_case("HTTP取回", status=r.status_code, cid_header=r.headers.get("X-Content-Cid"))
    assert r.status_code == 200
    assert r.content == data
    assert r.headers["X-Content-Sha256"] == cid

    # 未提交 -> 404
    assert client.get(f"/contents/{'0' * 64}/bytes").status_code == 404

    # 在线副本全部损坏 -> 503
    reg.tamper_replica(cid, "n1", b"bad")
    reg.tamper_replica(cid, "n2", b"bad")
    r503 = client.get(f"/contents/{cid}/bytes")
    log_case("HTTP无健康副本", status=r503.status_code, error=r503.json().get("error_type"))
    assert r503.status_code == 503
    assert r503.json()["error_type"] == "ReplicaUnavailableError"

    # HTTP 修复报告应说明替换节点与有效副本数
    rrep = client.post(f"/contents/{cid}/repair").json()
    log_case("HTTP修复坏副本", replaced=rrep["replaced_nodes"], valid=rrep["valid_replica_count"])
    assert sorted(rrep["replaced_nodes"]) == ["n1", "n2"]
    assert rrep["valid_replica_count"] == 2
    assert client.get(f"/contents/{cid}/bytes").content == data


def test_repair_preserves_only_healthy_sample_and_does_not_read_bad_as_source(registry):
    """n1 坏 + n2 健康：修复期间坏节点不得作为来源，唯一健康样本 n2 原样保留。"""
    _cluster(registry)
    data = b"only-good-source"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    n2_used_before = registry.get_node("n2").used_bytes
    registry.tamper_replica(cid, "n1", b"garbage-garbage")

    rep = RepairManager(registry).repair(cid)
    log_case("唯一样本保护", replaced=rep.replaced_nodes, reason=rep.reason)
    # n2 健康样本仍在、字节与记账都不变；n1 被正确字节替换
    assert registry.get_node("n2").read_replica(cid) == data
    assert registry.get_node("n2").used_bytes == n2_used_before
    assert registry.get_node("n1").read_replica(cid) == data
    assert rep.replaced_nodes == ("n1",) and rep.target_nodes == ()


def test_offline_corrupt_marker_does_not_poison_after_online_recovery_check(registry):
    """坏节点上的字节被外部修好后，下一次判定必须解除隔离（标记不陈旧）。"""
    _cluster(registry)
    data = b"marker-must-refresh"
    cid = registry.submit_content(data).cid
    registry.place_replica(cid, "n1")
    registry.place_replica(cid, "n2")
    registry.tamper_replica(cid, "n1", b"bad")
    registry.check_retrievability(cid)
    assert registry.corrupted_nodes(cid) == frozenset({"n1"})
    # 模拟外部把字节改回正确内容（带外修复/换盘恢复）
    registry.get_node("n1").rewrite_replica(cid, data)
    res = registry.check_retrievability(cid)
    log_case("隔离标记刷新", online=res.online_replica_count, corrupt=res.online_corrupted_count)
    assert res.online_replica_count == 2 and res.online_corrupted_count == 0
    assert registry.corrupted_nodes(cid) == frozenset()
