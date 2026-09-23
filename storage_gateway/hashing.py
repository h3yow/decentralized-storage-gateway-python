"""内容摘要与校验工具。"""

from __future__ import annotations

import hashlib


def sha256_hex(data: bytes) -> str:
    """计算字节内容的 SHA-256 十六进制摘要（即 CID）。"""
    return hashlib.sha256(data).hexdigest()


def verify_checksum(data: bytes, expected: str) -> bool:
    """校验数据摘要是否与期望摘要一致，不一致说明内容损坏。

    比较时忽略大小写与首尾空白，兼容常见的十六进制书写形式。
    """
    if not expected:
        return False
    actual = sha256_hex(data)
    return actual == expected.strip().lower()
