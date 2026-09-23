"""内容寻址：归一、空内容拒绝、校验失败按原因区分。"""

from __future__ import annotations

import pytest

from storage_gateway.errors import (
    ChecksumMismatchError,
    ContentNotFoundError,
    EmptyContentError,
)
from storage_gateway.hashing import sha256_hex

from .conftest import log_case


def test_identical_content_dedupes_to_same_cid(registry):
    data = b"same-content-block"
    log_case("内容归一", action="submit twice", size=len(data))
    m1 = registry.submit_content(data)
    m2 = registry.submit_content(data)
    log_case("内容归一", cid=m1.cid, same_cid=m1.cid == m2.cid)
    assert m1.cid == sha256_hex(data)
    assert m1.cid == m2.cid
    assert registry.total_replicas_system_wide() == 0


def test_different_content_get_distinct_cid(registry):
    a = registry.submit_content(b"aaa")
    b = registry.submit_content(b"bbb")
    log_case("不同内容不同CID", cid_a=a.cid[:12], cid_b=b.cid[:12])
    assert a.cid != b.cid


def test_empty_content_rejected(registry):
    log_case("空内容", data=b"")
    with pytest.raises(EmptyContentError):
        registry.submit_content(b"")


def test_corrupted_checksum_rejected_with_reason(registry):
    data = b"important block"
    bad = sha256_hex(b"tampered block")
    log_case("内容损坏", declared=bad[:12], actual=sha256_hex(data)[:12])
    with pytest.raises(ChecksumMismatchError) as exc:
        registry.submit_content(data, declared_checksum=bad)
    log_case("内容损坏", error=type(exc.value).__name__, message=str(exc.value)[:80])
    assert "does not match" in str(exc.value)
    # 损坏内容不得进入注册表
    with pytest.raises(ContentNotFoundError):
        registry.get_metadata(sha256_hex(data))


def test_correct_declared_checksum_accepted(registry):
    data = b"verified"
    m = registry.submit_content(data, declared_checksum=sha256_hex(data))
    log_case("校验通过", cid=m.cid[:12])
    assert m.checksum == m.cid
