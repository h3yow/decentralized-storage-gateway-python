"""修复流程：对低于可检索阈值的内容补齐在线健康副本，并替换已损坏副本。"""

from __future__ import annotations

from .errors import ContentNotFoundError
from .models import RepairReport
from .registry import ContentRegistry


class RepairManager:
    """根据当前注册表状态选择合规、在线、容量足够的节点补齐副本。

    修复与去重/放置/读取共享注册表锁，候选选择与实际放置为原子过程，
    因此并发修复不会对同一节点重复复制。修复的数据来源始终是注册表
    保存的可信样本，从不读取任何副本节点，坏数据不会随修复扩散。
    已被判定损坏的副本会被移除（释放容量）并由健康副本替换；
    健康副本绝不删除或移动，唯一健康样本始终保留。
    """

    def __init__(self, registry: ContentRegistry) -> None:
        self._registry = registry

    def repair(self, cid: str) -> RepairReport:
        registry = self._registry
        # 判定 + 损坏副本移除 + 补齐全部在注册表锁的串行化保护下完成，
        # 因此并发修复/读取彼此不会观察到中间态。
        with registry.lock():
            try:
                result = registry.check_retrievability(cid)
            except ContentNotFoundError:
                raise
            corrupt_nodes = registry.corrupt_nodes_for(cid)

            if result.retrievable and not corrupt_nodes:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=result.online_replica_count,
                    after_online_count=result.online_replica_count,
                    reason=(
                        f"无需修复：在线副本数 {result.online_replica_count} "
                        f">= 阈值 {result.threshold}"
                    ),
                    valid_nodes=self._valid_nodes(cid),
                )

            # 1) 移除已判定损坏的副本：释放容量、清除损坏登记。
            #    坏副本不再计入任何计数，也不会被当作修复来源。
            replaced: list[str] = []
            for node_id in corrupt_nodes:
                registry.remove_replica(cid, node_id)
                replaced.append(node_id)

            # 健康在线副本数不受坏副本移除影响（坏副本本就不计入）
            before_online = result.online_replica_count
            remaining_total = registry.total_replica_count(cid)

            # 2) 剩余副本全部离线时不修复：没有在线健康样本，
            #    唯一样本原样保留在原节点，绝不移动或删除。
            if before_online == 0 and remaining_total > 0:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=0,
                    after_online_count=0,
                    reason="修复失败：全部副本所在节点离线，无在线健康样本可复制",
                    replaced_corrupt_nodes=tuple(replaced),
                    valid_nodes=self._valid_nodes(cid),
                )
            if remaining_total == 0:
                # 注册表仍持有原始提交样本，可从该样本恢复首份副本
                registry.content_data(cid)

            # 3) 补齐健康在线副本到阈值（need 不超过阈值缺口，
            #    修复后有效副本数不会超出修复目标）。被移除坏副本的节点
            #    若在线且容量足够，会作为普通候选被重新选中，即坏副本被替换。
            candidates = self._eligible_candidates(cid)
            chosen: list[str] = []
            need = result.threshold - before_online
            for _ in range(max(0, need)):
                node_id = registry.place_on_first_eligible_node(cid, candidates)
                if node_id is None:
                    break
                chosen.append(node_id)

            after = registry.check_retrievability(cid)
            valid_nodes = self._valid_nodes(cid)
            if not chosen and not replaced:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=before_online,
                    after_online_count=after.online_replica_count,
                    reason="修复失败：没有合规且容量足够的在线节点可放置新副本",
                    valid_nodes=valid_nodes,
                )

            actions: list[str] = []
            if chosen:
                actions.append(f"新增副本 {chosen}")
            if replaced:
                actions.append(f"替换损坏副本所在节点 {replaced}")
            action_desc = "，".join(actions)
            if after.retrievable:
                reason = (
                    f"修复成功：{action_desc}，在线副本数 "
                    f"{before_online} -> {after.online_replica_count}，达到阈值 "
                    f"{after.threshold}"
                )
            else:
                reason = (
                    f"部分修复：{action_desc}，在线副本数 "
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
                replaced_corrupt_nodes=tuple(replaced),
                valid_nodes=valid_nodes,
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

    def _valid_nodes(self, cid: str) -> tuple[str, ...]:
        """当前持有该内容有效（未损坏）副本的节点 id，按序快照。"""
        return tuple(
            sorted(
                n.node_id for n in self._registry.list_nodes() if n.has(cid)
            )
        )
