"""HTTP 入口集成测试：错误原因在响应体中可区分。"""

from __future__ import annotations

from fastapi.testclient import TestClient

import main
from gateway import GatewayConfig, LocalStorageNode, StorageGateway


def make_client() -> TestClient:
    main.gateway = StorageGateway(GatewayConfig(min_replicas=2))
    g = main.gateway
    g.register_node(LocalStorageNode("n1", 100))
    g.register_node(LocalStorageNode("n2", 100))
    g.register_node(LocalStorageNode("n3", 100))
    g.register_node(LocalStorageNode("n4", 5))
    g.register_node(LocalStorageNode("bad", 100, authorized=False))
    return TestClient(main.app)


def test_full_flow_dedup_placement_retrievability_repair():
    client = make_client()
    payload = b"api-content"
    r = client.post("/contents", json={"data_hex": payload.hex()})
    assert r.status_code == 201
    cid = r.json()["cid"]

    r2 = client.post("/contents", json={"data_hex": payload.hex()})
    assert r2.status_code == 201 and r2.json()["cid"] == cid
    assert client.get("/stats").json()["total_replicas_logical"] == 0

    assert client.post(f"/contents/{cid}/replicas", json={"node_id": "n1"}).status_code == 201
    assert client.get(f"/contents/{cid}/retrievability").json()["status"] == "unretrievable"
    assert client.post(f"/contents/{cid}/replicas", json={"node_id": "n2"}).status_code == 201
    assert client.get(f"/contents/{cid}/retrievability").json()["status"] == "retrievable"

    client.post("/nodes/n1/status", json={"online": False})
    assert client.get(f"/contents/{cid}/retrievability").json()["status"] == "unretrievable"
    assert client.post(f"/contents/{cid}/repair").status_code == 200
    assert client.get(f"/contents/{cid}/retrievability").json()["status"] == "retrievable"

    body = client.get(f"/contents/{cid}").json()
    assert bytes.fromhex(body["data_hex"]) == payload


def test_error_reasons_are_distinguishable():
    client = make_client()
    cid = client.post("/contents", json={"data_hex": (b"x" * 50).hex()}).json()["cid"]

    r = client.post("/contents", json={"data_hex": b"x".hex(), "expected_checksum": "00"})
    assert r.status_code == 422 and r.json()["detail"]["reason"] == "checksum_mismatch"

    r = client.post(f"/contents/{cid}/replicas", json={"node_id": "n4"})
    assert r.status_code == 507 and r.json()["detail"]["reason"] == "capacity_exceeded"

    r = client.post(f"/contents/{cid}/replicas", json={"node_id": "bad"})
    assert r.status_code == 403 and r.json()["detail"]["reason"] == "unauthorized_node"

    client.post("/contents", json={"data_hex": b"y".hex()})
    ycid = client.post("/contents", json={"data_hex": b"y".hex()}).json()["cid"]
    client.post(f"/contents/{ycid}/replicas", json={"node_id": "n1"})
    r = client.post(f"/contents/{ycid}/replicas", json={"node_id": "n1"})
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "duplicate_placement"

    client.post("/nodes/n2/status", json={"online": False})
    r = client.post(f"/contents/{ycid}/replicas", json={"node_id": "n2"})
    assert r.status_code == 409 and r.json()["detail"]["reason"] == "node_offline"
