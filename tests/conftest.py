"""测试公共夹具与日志工具：每个用例打印输入与判定依据。"""

from __future__ import annotations

import logging

import pytest

from storage_gateway.gateway import StorageGateway
from storage_gateway.node import StorageNode
from storage_gateway.registry import ContentRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("storage-tests")


def log_case(title: str, **fields) -> None:
    parts = " | ".join(f"{k}={v!r}" for k, v in fields.items())
    logger.info("用例[%s] 输入/依据 -> %s", title, parts)


@pytest.fixture()
def registry() -> ContentRegistry:
    return ContentRegistry(retrieval_threshold=2)


@pytest.fixture()
def gateway() -> StorageGateway:
    return StorageGateway(retrieval_threshold=2)


def make_nodes(reg: ContentRegistry, spec: dict[str, int]) -> None:
    """按 {node_id: capacity} 批量注册默认授权在线节点。"""
    for node_id, cap in spec.items():
        reg.register_node(StorageNode(node_id, cap))


# 测试文件 -> 业务名称：终端汇总时按业务分组展示各用例通过情况
_BUSINESS_NAMES = {
    "test_content_addressing.py": "内容寻址/提交去重",
    "test_placement.py": "副本放置拒绝",
    "test_node_lifecycle.py": "节点上下线计数",
    "test_retrievability_repair.py": "可检索性判定与阈值修复",
    "test_retrieval.py": "内容读取与坏副本治理",
    "test_repair_flapping.py": "节点抖动×修复交叉（防超量/记账守恒）",
    "test_concurrency.py": "高并发守恒",
    "test_api.py": "HTTP 接口集成",
}


def pytest_terminal_summary(terminalreporter) -> None:  # noqa: ANN001
    """在测试结束后按业务分组打印用例通过/失败统计。"""
    import os

    stats: dict[str, dict[str, set[str]]] = {}
    for report in terminalreporter.stats.get("passed", []):
        fname = os.path.basename(str(report.fspath))
        stats.setdefault(fname, {}).setdefault("passed", set()).add(
            report.head_line.split("[")[0]
        )
    for key in ("failed", "error"):
        for report in terminalreporter.stats.get(key, []):
            fname = os.path.basename(str(report.fspath))
            stats.setdefault(fname, {}).setdefault(key, set()).add(
                report.head_line.split("[")[0]
            )

    terminalreporter.write_sep("-", "按业务模块统计用例通过情况")
    for fname in sorted(stats):
        passed = stats[fname].get("passed", set())
        failed = stats[fname].get("failed", set()) | stats[fname].get("error", set())
        business = _BUSINESS_NAMES.get(fname, fname)
        terminalreporter.write_line(
            f"{business}（{fname}）：通过 {len(passed)} / 失败 {len(failed)}"
        )
        for name in sorted(passed):
            terminalreporter.write_line(f"    PASS {name}")
        for name in sorted(failed):
            terminalreporter.write_line(f"    FAIL {name}")
