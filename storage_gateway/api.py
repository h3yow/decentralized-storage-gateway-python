"""FastAPI HTTP 入口：把网关门面暴露为 REST 接口，错误映射为可区分状态码。"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from .config import GatewayConfig
from .errors import (
    ChecksumMismatchError,
    ContentNotFoundError,
    ContentUnavailableError,
    DuplicatePlacementError,
    EmptyContentError,
    NodeNotFoundError,
    CapacityExceededError,
    UnauthorizedNodeError,
    NodeOfflineError,
    StorageError,
)
from .gateway import StorageGateway
from .models import NodeStatus

# 错误类型 -> HTTP 状态码；同类失败使用同一码，原因在 error_type/message 中区分
_ERROR_STATUS: dict[type[StorageError], int] = {
    EmptyContentError: 400,
    ChecksumMismatchError: 422,
    ContentNotFoundError: 404,
    ContentUnavailableError: 503,
    NodeNotFoundError: 404,
    UnauthorizedNodeError: 403,
    NodeOfflineError: 409,
    CapacityExceededError: 507,
    DuplicatePlacementError: 409,
}


class SubmitRequest(BaseModel):
    # 原始字节以 base64 字符串传输；也接受 utf-8 文本由调用方自行编码
    content_b64: str = ""
    content_text: str | None = None
    declared_checksum: str | None = None


class NodeCreateRequest(BaseModel):
    node_id: str
    # -1 表示未指定，使用 GatewayConfig.default_node_capacity
    capacity: int = Field(default=-1, ge=-1)
    authorized: bool = True


class StatusUpdateRequest(BaseModel):
    status: NodeStatus


class ReplicateRequest(BaseModel):
    node_ids: list[str]


def create_app(
    gateway: StorageGateway | None = None,
    config: GatewayConfig | None = None,
) -> FastAPI:
    gw = gateway or StorageGateway(config=config)
    app = FastAPI(title="Local Decentralized Storage Gateway", version="0.1.0")

    @app.exception_handler(StorageError)
    async def _storage_error_handler(_request, exc: StorageError):  # type: ignore[no-untyped-def]
        status_code = _ERROR_STATUS.get(type(exc), 400)
        return JSONResponse(
            status_code=status_code,
            content={
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
        )

    @app.get("/")
    def read_root():
        return {"service": "decentralized-storage-gateway", "status": "ok"}

    @app.post("/nodes")
    def create_node(req: NodeCreateRequest):
        from .node import StorageNode

        node = StorageNode(
            node_id=req.node_id,
            capacity=(
                req.capacity
                if req.capacity >= 0
                else gw.config.default_node_capacity
            ),
            authorized=req.authorized,
        )
        gw.registry.register_node(node)
        return {"node_id": node.node_id, "capacity": node.capacity}

    @app.post("/nodes/{node_id}/status")
    def update_node_status(node_id: str, req: StatusUpdateRequest):
        gw.registry.set_node_status(node_id, req.status)
        return {"node_id": node_id, "status": req.status.value}

    @app.post("/contents")
    def submit_content(req: SubmitRequest):
        import base64

        if req.content_text is not None:
            data = req.content_text.encode("utf-8")
        else:
            data = base64.b64decode(req.content_b64)
        meta = gw.submit(data, req.declared_checksum)
        return _metadata_json(meta)

    @app.post("/contents/{cid}/replicas")
    def replicate(cid: str, req: ReplicateRequest):
        return {"cid": cid, "results": gw.replicate(cid, req.node_ids)}

    @app.put("/contents/{cid}/replicas/{node_id}")
    def place_single_replica(cid: str, node_id: str):
        """单节点放置：失败直接抛出，由异常处理器映射为可区分状态码。"""
        gw.registry.place_replica(cid, node_id)
        return {"cid": cid, "node_id": node_id, "result": "ok"}

    @app.get("/contents/{cid}")
    def retrieve_content(cid: str):
        """按内容标识取回原始字节。

        404 = 从未提交过；503 = 有记录但当前没有健康在线副本可取。
        """
        data = gw.retrieve(cid)
        return Response(content=data, media_type="application/octet-stream")

    @app.get("/contents/{cid}/status")
    def content_status(cid: str):
        result = gw.status(cid)
        return {
            "cid": result.cid,
            "retrievable": result.retrievable,
            "online_replica_count": result.online_replica_count,
            "total_replica_count": result.total_replica_count,
            "threshold": result.threshold,
            "reasons": list(result.reasons),
            "corrupt_replica_count": result.corrupt_replica_count,
            # 有效（未损坏、含离线好副本）副本数：补副本判定口径
            "healthy_replica_count": result.healthy_replica_count,
        }

    @app.post("/contents/{cid}/repair")
    def repair(cid: str):
        report = gw.repair(cid)
        return {
            "cid": report.cid,
            "repaired": report.repaired,
            "before_online_count": report.before_online_count,
            "after_online_count": report.after_online_count,
            "target_nodes": list(report.target_nodes),
            "reason": report.reason,
            "replaced_corrupt_nodes": list(report.replaced_corrupt_nodes),
            "valid_nodes": list(report.valid_nodes),
            "target_replica_count": report.target_replica_count,
            "healthy_replica_count": report.healthy_replica_count,
            "offline_valid_nodes": list(report.offline_valid_nodes),
        }

    return app


def _metadata_json(meta) -> dict:  # noqa: ANN001
    return {
        "cid": meta.cid,
        "size": meta.size,
        "checksum": meta.checksum,
        "replica_nodes": sorted(meta.replica_nodes),
    }
