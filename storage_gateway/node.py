"""本地存储节点：模拟容量、授权状态、上下线与本地副本字节保存。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import CapacityExceededError, DuplicatePlacementError, NodeOfflineError
from .models import NodeStatus


@dataclass
class StorageNode:
    """单个本地存储节点。

    每个节点自带可重入锁，节点自身的容量记账与副本字节变更持锁完成，
    供上层在并发放置/删除/重写/上下线时保证计数不发生错乱。

    副本以 CID -> 字节 的形式真实保存；容量记账独立记录每个副本"承诺的
    内容大小"（即提交时的原始长度），因此落盘字节被外部篡改、或随后用
    正确字节重写替换，都不会让 used_bytes 重复计费或对不上账。
    """

    node_id: str
    capacity: int
    authorized: bool = True
    status: NodeStatus = NodeStatus.ONLINE
    used_bytes: int = field(default=0)
    _replicas: dict[str, bytes] = field(default_factory=dict, repr=False)
    _charged_sizes: dict[str, int] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def has(self, cid: str) -> bool:
        """该节点是否存有指定内容副本（含已被登记为损坏的副本占位）。"""
        with self._lock:
            return cid in self._replicas

    def place(self, cid: str, data: bytes) -> None:
        """在节点上放置一个内容副本。

        离线 / 重复放置 / 容量不足均显式报错，绝不静默接受。
        """
        with self._lock:
            if self.status == NodeStatus.OFFLINE:
                raise NodeOfflineError(f"node {self.node_id} is offline")
            if cid in self._replicas:
                raise DuplicatePlacementError(
                    f"content {cid} already exists on node {self.node_id}"
                )
            size = len(data)
            if self.used_bytes + size > self.capacity:
                raise CapacityExceededError(
                    f"node {self.node_id} capacity exceeded: "
                    f"need {size}, free {self.capacity - self.used_bytes}"
                )
            self._replicas[cid] = bytes(data)
            self._charged_sizes[cid] = size
            self.used_bytes += size

    def remove(self, cid: str) -> int:
        """移除副本并按承诺大小归还容量；副本不存在时为幂等空操作。

        返回实际归还的字节数。
        """
        with self._lock:
            if cid not in self._replicas:
                return 0
            self._replicas.pop(cid, None)
            charged = self._charged_sizes.pop(cid, 0)
            self.used_bytes = max(0, self.used_bytes - charged)
            return charged

    def read_replica(self, cid: str) -> bytes | None:
        """读取节点上保存的副本字节；不存在返回 None（不校验字节正确性）。"""
        with self._lock:
            data = self._replicas.get(cid)
            return None if data is None else bytes(data)

    def rewrite_replica(self, cid: str, data: bytes) -> None:
        """用正确字节就地替换已有副本（修复坏副本）。

        替换不改变容量记账：该副本承诺占用的空间与放置时一致，
        因此绝不会因为替换而重复计费。
        """
        with self._lock:
            if cid not in self._replicas:
                raise KeyError(
                    f"content {cid} not present on node {self.node_id}"
                )
            self._replicas[cid] = bytes(data)

    def tamper_replica(self, cid: str, bad_data: bytes) -> None:
        """模拟落盘字节被带外篡改：只替换保存的字节，不动任何容量记账。

        仅供故障注入（测试/运维演练）使用；节点是否在线不影响磁盘被改。
        """
        with self._lock:
            if cid not in self._replicas:
                raise KeyError(
                    f"content {cid} not present on node {self.node_id}"
                )
            self._replicas[cid] = bytes(bad_data)

    def replicas(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._replicas.keys())

    def replica_count(self) -> int:
        with self._lock:
            return len(self._replicas)

    def free_bytes(self) -> int:
        with self._lock:
            return self.capacity - self.used_bytes

    def set_status(self, status: NodeStatus) -> None:
        with self._lock:
            # 归一化：兼容调用方传入裸字符串 "online"/"offline"，
            # 内部始终保存真正的枚举成员。
            self.status = NodeStatus(status)
