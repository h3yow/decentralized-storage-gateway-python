"""本地存储节点：模拟容量、授权状态、上下线与本地副本字节保存。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import (
    CapacityExceededError,
    ChecksumMismatchError,
    ContentNotFoundError,
    DuplicatePlacementError,
    NodeOfflineError,
)
from .hashing import sha256_hex
from .models import NodeStatus


@dataclass
class StorageNode:
    """单个本地存储节点。

    节点真实保存副本字节（cid -> data），因此可以模拟"落盘数据被篡改"
    并在读取时按内容标识校验。每个节点自带可重入锁，节点自身的容量记账
    与副本集合变更持锁完成，供上层在并发放置/删除/上下线时保证计数不发生错乱。
    """

    node_id: str
    capacity: int
    authorized: bool = True
    status: NodeStatus = NodeStatus.ONLINE
    used_bytes: int = field(default=0)
    _replicas: dict[str, bytes] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def has(self, cid: str) -> bool:
        """该节点是否存有指定内容副本（不关心字节是否仍完整）。"""
        with self._lock:
            return cid in self._replicas

    def place(self, cid: str, size: int, data: bytes) -> None:
        """在节点上放置一个副本。

        离线 / 重复放置 / 容量不足 / 字节与内容标识不符 均显式报错，绝不静默接受。
        放置时校验 data 的摘要确实等于 cid，坏字节从入口处就不会被当成副本。
        """
        with self._lock:
            if self.status is NodeStatus.OFFLINE:
                raise NodeOfflineError(f"node {self.node_id} is offline")
            if cid in self._replicas:
                raise DuplicatePlacementError(
                    f"content {cid} already exists on node {self.node_id}"
                )
            if self.used_bytes + size > self.capacity:
                raise CapacityExceededError(
                    f"node {self.node_id} capacity exceeded: "
                    f"need {size}, free {self.capacity - self.used_bytes}"
                )
            if sha256_hex(data) != cid:
                raise ChecksumMismatchError(
                    f"replica bytes do not match content id {cid} "
                    f"on node {self.node_id}"
                )
            self._replicas[cid] = bytes(data)
            self.used_bytes += size

    def read(self, cid: str) -> bytes:
        """读取本地保存的副本字节；离线或副本不存在均显式报错。

        返回的是节点实际保存的字节，是否仍与 cid 匹配由调用方校验。
        """
        with self._lock:
            if self.status is NodeStatus.OFFLINE:
                raise NodeOfflineError(f"node {self.node_id} is offline")
            try:
                return self._replicas[cid]
            except KeyError:
                raise ContentNotFoundError(
                    f"content {cid} has no replica on node {self.node_id}"
                ) from None

    def overwrite_data(self, cid: str, data: bytes) -> None:
        """直接覆写本地副本字节——仅用于模拟"落盘数据被篡改"的场景。

        不改变容量记账（副本仍占用原大小的配额），模拟现实中等长/异长
        的底层数据损坏；上层应通过读取校验发现它，而不是通过本方法。
        """
        with self._lock:
            if cid not in self._replicas:
                raise ContentNotFoundError(
                    f"content {cid} has no replica on node {self.node_id}"
                )
            self._replicas[cid] = bytes(data)

    def remove(self, cid: str, size: int) -> None:
        """移除副本并归还容量；副本不存在时为幂等空操作。"""
        with self._lock:
            if cid not in self._replicas:
                return
            del self._replicas[cid]
            self.used_bytes = max(0, self.used_bytes - size)

    def replicas(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._replicas)

    def replica_count(self) -> int:
        with self._lock:
            return len(self._replicas)

    def free_bytes(self) -> int:
        with self._lock:
            return self.capacity - self.used_bytes

    def set_status(self, status: NodeStatus) -> None:
        with self._lock:
            self.status = status
