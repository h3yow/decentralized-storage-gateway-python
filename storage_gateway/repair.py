"""修复流程：对低于可检索阈值的内容补齐在线健康副本。"""

from __future__ import annotations

from .errors import ContentNotFoundError
from .models import RepairReport
from .registry import ContentRegistry


class RepairManager:
    """根据当前注册表状态选择合规、在线、容量足够的节点补齐副本。

    修复与去重/放置共享注册表锁，候选选择与实际放置为原子过程，
    因此并发修复不会对同一节点重复复制，也不会删除/移动唯一健康样本
    （修复只做"增加副本"，从不删除现有副本；来源样本始终保留）。
    """

    def __init__(self, registry: ContentRegistry) -> None:
        self._registry = registry

    def repair(self, cid: str) -> RepairReport:
        registry = self._registry
        # 判定 + 补齐全部在注册表锁的串行化保护下完成，
        # 因此并发修复彼此不会观察到中间态。
        with registry.lock():
            try:
                result = registry.check_retrievability(cid)
            except ContentNotFoundError:
                raise

            if result.retrievable:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=result.online_replica_count,
                    after_online_count=result.online_replica_count,
                    reason=(
                        f"无需修复：在线副本数 {result.online_replica_count} "
                        f">= 阈值 {result.threshold}"
                    ),
                )

            # 必须存在至少一个在线健康副本作为修复来源；
            # 注册表保存的原始内容即可信样本，唯一样本绝不移动或删除。
            before_online = result.online_replica_count
            if before_online == 0 and result.total_replica_count > 0:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=0,
                    after_online_count=0,
                    reason="修复失败：全部副本所在节点离线，无在线健康样本可复制",
                )
            if result.total_replica_count == 0:
                # 注册表仍持有原始提交样本，可从该样本恢复首份副本
                registry.content_data(cid)

            candidates = self._eligible_candidates(cid)
            chosen: list[str] = []
            need = result.threshold - result.online_replica_count
            for _ in range(need):
                node_id = registry.place_on_first_eligible_node(cid, candidates)
                if node_id is None:
                    break
                chosen.append(node_id)

            after = registry.check_retrievability(cid)
            if not chosen:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=before_online,
                    after_online_count=after.online_replica_count,
                    reason="修复失败：没有合规且容量足够的在线节点可放置新副本",
                )

            if after.retrievable:
                reason = (
                    f"修复成功：新增副本 {chosen}，在线副本数 "
                    f"{before_online} -> {after.online_replica_count}，达到阈值 "
                    f"{after.threshold}"
                )
            else:
                reason = (
                    f"部分修复：新增副本 {chosen}，在线副本数 "
                    f"{before_online} -> {after.online_replica_count}，仍低于阈值 "
                    f"{after.threshold}（合规节点/容量不足）"
                )
            return RepairReport(
                cid=cid,
                repaired=after.retrievable,
                before_online_count=before_online,
                after_online_count=after.online_replica_count,
                target_nodes=tuple(chosen),
                reason=reason,
            )

    def repair_all(self) -> list[RepairReport]:
        # 先取快照再逐个修，避免持锁遍历期间修改字典
        cids = self._registry.list_content_ids()
        return [self.repair(cid) for cid in cids]

    # ---- 内部 ----
    def _eligible_candidates(self, cid: str) -> tuple[str, ...]:
        registry = self._registry
        nodes = list(registry.list_nodes())
        # 确定性顺序：节点 id 排序，保证修复结论可解释、可复现
        nodes.sort(key=lambda n: n.node_id)
        return tuple(n.node_id for n in nodes)
