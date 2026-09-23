"""HTTP 入口：把网关能力暴露为 REST API，错误原因映射为可区分的状态码。"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from gateway import (
    FailureReason,
    GatewayConfig,
    GatewayError,
    LocalStorageNode,
    StorageGateway,
)

app = FastAPI(title="Decentralized Storage Gateway")
gateway = StorageGateway(GatewayConfig())

_STATUS_BY_REASON = {
    FailureReason.CHECKSUM_MISMATCH: 422,
    FailureReason.CONTENT_CORRUPTED: 422,
    FailureReason.CAPACITY_EXCEEDED: 507,
    FailureReason.DUPLICATE_PLACEMENT: 409,
    FailureReason.UNAUTHORIZED_NODE: 403,
    FailureReason.NODE_OFFLINE: 409,
    FailureReason.NODE_NOT_FOUND: 404,
    FailureReason.CONTENT_NOT_FOUND: 404,
    FailureReason.INSUFFICIENT_REPLICAS: 409,
    FailureReason.NO_REPAIR_SOURCE: 409,
}


def _translate(exc: GatewayError) -> HTTPException:
    return HTTPException(
        status_code=_STATUS_BY_REASON.get(exc.reason, 400),
        detail={"reason": exc.reason.value, "detail": exc.detail},
    )


class NodeIn(BaseModel):
    node_id: str
    capacity: int
    authorized: bool = True


class SubmitIn(BaseModel):
    data_hex: str
    expected_checksum: str | None = None


class PlacementIn(BaseModel):
    node_id: str


class NodeStatusIn(BaseModel):
    online: bool


@app.get("/")
def read_root():
    return {"service": "decentralized-storage-gateway", "min_replicas": gateway.config.min_replicas}


@app.post("/nodes", status_code=201)
def register_node(body: NodeIn):
    try:
        gateway.register_node(LocalStorageNode(body.node_id, body.capacity, body.authorized))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"node_id": body.node_id}


@app.get("/nodes")
def list_nodes():
    return [
        {
            "node_id": n.node_id,
            "capacity": n.capacity,
            "used": n.used,
            "authorized": n.authorized,
            "status": n.status.value,
        }
        for n in gateway.list_nodes()
    ]


@app.post("/nodes/{node_id}/status")
def set_node_status(node_id: str, body: NodeStatusIn):
    try:
        gateway.set_node_status(node_id, body.online)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {"node_id": node_id, "online": body.online}


@app.post("/contents", status_code=201)
def submit(body: SubmitIn):
    try:
        cid = gateway.submit(bytes.fromhex(body.data_hex), body.expected_checksum)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {"cid": cid, "status": gateway.retrievability(cid).value}


@app.get("/contents/{cid}")
def get_content(cid: str):
    try:
        data = gateway.get(cid)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {"cid": cid, "data_hex": data.hex()}


@app.post("/contents/{cid}/replicas", status_code=201)
def place_replica(cid: str, body: PlacementIn):
    try:
        gateway.place_replica(cid, body.node_id)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {"cid": cid, "node_id": body.node_id}


@app.get("/contents/{cid}/retrievability")
def retrievability(cid: str):
    try:
        status = gateway.retrievability(cid)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {
        "cid": cid,
        "status": status.value,
        "online_replicas": gateway.replica_count(cid),
        "min_replicas": gateway.config.min_replicas,
    }


@app.post("/contents/{cid}/repair")
def repair(cid: str):
    try:
        status = gateway.repair(cid)
    except GatewayError as exc:
        raise _translate(exc) from exc
    return {"cid": cid, "status": status.value}


@app.get("/stats")
def stats():
    return {
        "total_replicas_online": gateway.total_replicas(online_only=True),
        "total_replicas_logical": gateway.total_replicas(online_only=False),
    }
