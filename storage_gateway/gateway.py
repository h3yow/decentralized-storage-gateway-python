"""服务门面：对外提供提交内容块、放置副本、状态判定、修复等一致入口。"""

from __future__ import annotations

from .config import GatewayConfig
from .errors import StorageError
from .models import ContentMetadata, RepairReport, RetrievabilityResult
from .registry import ContentRegistry
from .repair import RepairManager


class StorageGateway:
    """门面：聚合注册表与修复器，所有入口共用同一套并发与判定规则。

    可直接传入检索阈值，或传入完整 GatewayConfig。
    """

    def __init__(
        self,
        retrieval_threshold: int = 2,
        config: GatewayConfig | None = None,
    ) -> None:
        cfg = config or GatewayConfig(retrieval_threshold=retrieval_threshold)
        self._config = cfg
        self._registry = ContentRegistry(
            retrieval_threshold=cfg.retrieval_threshold
        )
        self._repair = RepairManager(self._registry)

    @property
    def config(self) -> GatewayConfig:
        return self._config

    @property
    def registry(self) -> ContentRegistry:
        return self._registry

    def submit(
        self, data: bytes, declared_checksum: str | None = None
    ) -> ContentMetadata:
        """提交内容（损坏/空内容直接抛错），重复内容归一到同一 CID。"""
        return self._registry.submit_content(data, declared_checksum)

    def replicate(self, cid: str, node_ids: list[str]) -> dict[str, str]:
        """向多个节点复制，逐节点给出 "ok" 或 "rejected:<原因类型>" 结论。

        单个节点被拒绝不中断其余节点；内容/CID 本身非法时直接抛错。
        """
        results: dict[str, str] = {}
        for node_id in node_ids:
            try:
                self._registry.place_replica(cid, node_id)
                results[node_id] = "ok"
            except StorageError as exc:
                results[node_id] = f"rejected:{type(exc).__name__}:{exc}"
        return results

    def status(self, cid: str) -> RetrievabilityResult:
        return self._registry.check_retrievability(cid)

    def retrieve(self, cid: str) -> bytes:
        """按内容标识取回原始字节。

        只从在线健康副本读取并逐份校验；坏副本自动跳过并登记，
        取不到时按"从未提交"与"无健康副本"区分抛错。
        """
        return self._registry.retrieve_content(cid)

    def repair(self, cid: str) -> RepairReport:
        return self._repair.repair(cid)

    def repair_all(self) -> list[RepairReport]:
        return self._repair.repair_all()
