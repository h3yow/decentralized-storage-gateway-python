"""本地存储节点：模拟容量、授权状态、上下线与本地副本保存。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import CapacityExceededError, DuplicatePlacementError, NodeOfflineError
from .models import NodeStatus


@dataclass
class StorageNode:
    """单个本地存储节点。

    每个节点自带可重入锁，节点自身的容量记账与副本集合变更持锁完成，
    供上层在并发放置/删除/上下线时保证计数不发生错乱。
    """

    node_id: str
    capacity: int
    authorized: bool = True
    status: NodeStatus = NodeStatus.ONLINE
    used_bytes: int = field(default=0)
    _replicas: set[str] = field(default_factory=set, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def has(self, cid: str) -> bool:
        """该节点是否存有指定内容副本。"""
        with self._lock:
            return cid in self._replicas

    def place(self, cid: str, size: int) -> None:
        """在节点上放置一个副本。

        离线 / 重复放置 / 容量不足均显式报错，绝不静默接受。
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
            self._replicas.add(cid)
            self.used_bytes += size

    def remove(self, cid: str, size: int) -> None:
        """移除副本并归还容量；副本不存在时为幂等空操作。"""
        with self._lock:
            if cid not in self._replicas:
                return
            self._replicas.discard(cid)
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
