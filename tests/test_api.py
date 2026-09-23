"""HTTP 入口：错误类型映射为可区分状态码，判定结论可解释。"""

from __future__ import annotations

import base64

from fastapi.testclient import TestClient

from storage_gateway.api import create_app
from storage_gateway.gateway import StorageGateway
from storage_gateway.hashing import sha256_hex

from .conftest import log_case


def _client() -> TestClient:
    return TestClient(create_app(StorageGateway(retrieval_threshold=2)))


def test_full_flow_via_http():
    client = _client()
    # 建三个节点，其中一个未授权、一个容量极小
    for nid, cap, auth in [("n1", 1000, True), ("n2", 1000, True), ("n3", 1000, False)]:
        r = client.post("/nodes", json={"node_id": nid, "capacity": cap, "authorized": auth})
        assert r.status_code == 200

    data = b"http-block"
    r = client.post(
        "/contents",
        json={"content_b64": base64.b64encode(data).decode()},
    )
    assert r.status_code == 200
    cid = r.json()["cid"]
    log_case("HTTP提交", cid=cid[:12])

    # 损坏内容 -> 422
    r_bad = client.post(
        "/contents",
        json={"content_text": "tampered", "declared_checksum": "0" * 64},
    )
    log_case("HTTP损坏", status=r_bad.status_code, error=r_bad.json()["error_type"])
    assert r_bad.status_code == 422
    assert r_bad.json()["error_type"] == "ChecksumMismatchError"

    # 批量复制：ok / 未授权 403 类结论 / 未知节点 404 类结论
    r = client.post(f"/contents/{cid}/replicas", json={"node_ids": ["n1", "n3", "ghost"]})
    results = r.json()["results"]
    log_case("HTTP批量复制", results=results)
    assert results["n1"] == "ok"
    assert "UnauthorizedNodeError" in results["n3"]
    assert "NodeNotFoundError" in results["ghost"]

    # 单副本 -> 不可检索
    r = client.get(f"/contents/{cid}/status")
    body = r.json()
    log_case("HTTP状态", retrievable=body["retrievable"], reasons=body["reasons"])
    assert body["retrievable"] is False and body["online_replica_count"] == 1

    # 修复 -> 达到阈值
    r = client.post(f"/contents/{cid}/repair")
    rep = r.json()
    log_case("HTTP修复", repaired=rep["repaired"], targets=rep["target_nodes"])
    assert rep["repaired"] is True and rep["after_online_count"] == 2

    # 下线 n1：总量仍 2，在线 1，不可检索
    client.post("/nodes/n1/status", json={"status": "offline"})
    body = client.get(f"/contents/{cid}/status").json()
    log_case("HTTP下线", total=body["total_replica_count"], online=body["online_replica_count"])
    assert body["total_replica_count"] == 2 and body["online_replica_count"] == 1
    assert body["retrievable"] is False


def test_direct_error_status_codes():
    client = _client()
    client.post("/nodes", json={"node_id": "n1", "capacity": 1})
    data = b"x" * 100
    cid = sha256_hex(data)
    client.post("/contents", json={"content_b64": base64.b64encode(data).decode()})

    cases = [
        ("PUT", f"/contents/{cid}/replicas/n1", None, 507),    # 容量不足
        ("PUT", f"/contents/{cid}/replicas/nope", None, 404),  # 节点不存在
        ("GET", "/contents/unknown/status", None, 404),        # 内容不存在
    ]
    for method, path, payload, expected in cases:
        if method == "GET":
            r = client.get(path)
        else:
            r = client.put(path)
        log_case("HTTP错误码", path=path, status=r.status_code, expected=expected, error=r.json().get("error_type"))
        assert r.status_code == expected
