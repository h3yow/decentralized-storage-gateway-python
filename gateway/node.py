"""本地存储节点：容量、授权、上下线与字节存取。"""

from __future__ import annotations

import threading

from .errors import FailureReason, GatewayError
from .models import NodeInfo, NodeStatus


class LocalStorageNode:
    """模拟一个本地存储节点。

    - 容量不足、未授权、离线时的写入都会被拒绝并给出可区分原因；
    - 离线时读取同样被拒绝（副本不可达）；
    - 内部计数（used）由锁保护，并发写删不会错乱。
    """

    def __init__(self, node_id: str, capacity: int, authorized: bool = True) -> None:
        if capacity < 0:
            raise ValueError("capacity must be >= 0")
        self.info = NodeInfo(node_id=node_id, capacity=capacity, authorized=authorized)
        self._data: dict[str, bytes] = {}
        self._lock = threading.Lock()

    @property
    def node_id(self) -> str:
        return self.info.node_id

    @property
    def status(self) -> NodeStatus:
        return self.info.status

    def go_offline(self) -> None:
        with self._lock:
            self.info.status = NodeStatus.OFFLINE

    def go_online(self) -> None:
        with self._lock:
            self.info.status = NodeStatus.ONLINE

    def has(self, cid: str) -> bool:
        with self._lock:
            return cid in self._data

    def _check_writable(self) -> None:
        if not self.info.authorized:
            raise GatewayError(FailureReason.UNAUTHORIZED_NODE, f"node {self.node_id} is not authorized")
        if self.info.status is NodeStatus.OFFLINE:
            raise GatewayError(FailureReason.NODE_OFFLINE, f"node {self.node_id} is offline")

    def store(self, cid: str, data: bytes) -> None:
        with self._lock:
            self._check_writable()
            if cid in self._data:
                raise GatewayError(FailureReason.DUPLICATE_PLACEMENT, f"{cid} already on node {self.node_id}")
            size = len(data)
            if self.info.used + size > self.info.capacity:
                raise GatewayError(
                    FailureReason.CAPACITY_EXCEEDED,
                    f"node {self.node_id} used={self.info.used} need={size} capacity={self.info.capacity}",
                )
            self._data[cid] = bytes(data)
            self.info.used += size

    def fetch(self, cid: str) -> bytes:
        with self._lock:
            if self.info.status is NodeStatus.OFFLINE:
                raise GatewayError(FailureReason.NODE_OFFLINE, f"node {self.node_id} is offline")
            try:
                return self._data[cid]
            except KeyError:
                raise GatewayError(FailureReason.CONTENT_NOT_FOUND, f"{cid} not on node {self.node_id}") from None

    def remove(self, cid: str) -> None:
        with self._lock:
            data = self._data.pop(cid, None)
            if data is not None:
                self.info.used -= len(data)
