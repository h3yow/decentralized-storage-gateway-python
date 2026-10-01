"""错误类型定义：所有失败均显式区分原因，不静默接受。"""


class StorageError(Exception):
    """所有存储网关错误的基类。"""


# ---- 内容层错误 ----
class ContentError(StorageError):
    """内容相关错误基类。"""


class ChecksumMismatchError(ContentError):
    """提交内容的校验值与声称校验值不一致（数据损坏）。"""


class EmptyContentError(ContentError):
    """提交了空内容。"""


class ContentNotFoundError(ContentError):
    """引用的内容标识（CID）在注册表中不存在。"""


class ContentUnavailableError(ContentError):
    """内容有记录，但当前没有任何在线健康副本可供读取。

    与 ContentNotFoundError 严格区分：后者是"从未提交过"，
    本错误是"提交过、但副本全部离线或已被判定损坏"。
    """


# ---- 节点 / 放置层错误 ----
class PlacementError(StorageError):
    """副本放置相关错误基类。"""


class NodeNotFoundError(PlacementError):
    """目标节点不存在。"""


class UnauthorizedNodeError(PlacementError):
    """目标节点未获授权（不在允许节点集合中）。"""


class NodeOfflineError(PlacementError):
    """目标节点当前离线，无法放置。"""


class CapacityExceededError(PlacementError):
    """节点剩余容量不足以容纳该副本。"""


class DuplicatePlacementError(PlacementError):
    """该内容副本已存在于目标节点上，拒绝重复放置。"""


# ---- 修复层错误 ----
class RepairError(StorageError):
    """修复相关错误基类。"""


class NoHealthyReplicaError(RepairError):
    """没有任何在线健康副本可作为修复来源。"""


class NoReplacementNodeError(RepairError):
    """找不到可放置新副本的合规节点（容量不足/无授权节点等）。"""
