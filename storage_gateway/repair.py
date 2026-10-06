"""修复流程：把有效副本数补齐到目标份数，并移除/替换已损坏副本。

口径定义：
- **有效副本** = 节点持有且未被判定损坏的副本，不区分在线/离线；
- **目标份数** = 检索阈值 retrieval_threshold；
- 修复的不变式：修复结束后有效副本数 **不超过** 目标份数。
  暂时离线但数据完好的副本也计入有效副本——若把它们算上已经够数，
  就一份都不补（哪怕当前在线副本低于阈值、本次先不修），否则节点
  恢复上线后系统里的有效副本数会超出目标份数。
"""

from __future__ import annotations

from .errors import ContentNotFoundError
from .models import RepairReport
from .registry import ContentRegistry


class RepairManager:
    """根据当前注册表状态选择合规、在线、容量足够的节点补齐副本。

    修复与去重/放置/读取共享注册表锁，判定、坏副本移除、候选选择与
    实际放置为一个原子过程，因此并发修复/上下线抖动不会让有效副本数
    超出目标份数。修复的数据来源始终是注册表保存的可信样本，从不读取
    任何副本节点，坏数据不会随修复扩散。已被判定损坏的副本会被移除
    （释放容量）并按目标缺口补健康副本；健康副本绝不删除或移动，
    唯一健康样本始终保留。
    """

    def __init__(self, registry: ContentRegistry) -> None:
        self._registry = registry

    def repair(self, cid: str) -> RepairReport:
        registry = self._registry
        # 判定 + 损坏副本移除 + 补齐全部在注册表锁的串行化保护下完成，
        # 因此并发修复/读取/上下线彼此不会观察到中间态。
        with registry.lock():
            try:
                result = registry.check_retrievability(cid)
            except ContentNotFoundError:
                raise
            threshold = result.threshold
            corrupt_nodes = registry.corrupt_nodes_for(cid)
            effective_before = registry.effective_replica_count(cid)

            # 有效副本（含暂时离线的健康副本）已达目标份数且无坏副本：
            # 一份都不补。此时即便在线副本低于阈值（节点暂时离线）也先不修，
            # 否则离线的健康副本随节点恢复后，有效副本数会超出目标份数。
            if effective_before >= threshold and not corrupt_nodes:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=result.online_replica_count,
                    after_online_count=result.online_replica_count,
                    reason=(
                        f"无需修复：有效副本数 {effective_before} 已达目标份数 "
                        f"{threshold}（含暂时离线的健康副本；当前在线 "
                        f"{result.online_replica_count} 份）"
                    ),
                    valid_nodes=self._valid_nodes(cid),
                    after_effective_count=effective_before,
                    target_count=threshold,
                )

            # 1) 移除已判定损坏的副本：释放容量、清除损坏登记。
            #    坏副本不再计入任何计数，也不会被当作修复来源。
            #    移除与后续可能的重新放置是"先撤后补"，同一节点容量不会算重。
            replaced: list[str] = []
            for node_id in corrupt_nodes:
                registry.remove_replica(cid, node_id)
                replaced.append(node_id)

            # 健康在线副本数不受坏副本移除影响（坏副本本就不计入）
            before_online = result.online_replica_count
            effective = registry.effective_replica_count(cid)

            # 2) 有效副本全部离线时不补新副本：没有在线健康样本，
            #    唯一样本原样保留在原节点，绝不移动或删除。
            if before_online == 0 and effective > 0:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=0,
                    after_online_count=0,
                    reason=(
                        f"修复暂缓：全部 {effective} 个有效副本所在节点离线，"
                        "无在线健康样本可复制，原样保留等待节点恢复"
                    ),
                    replaced_corrupt_nodes=tuple(replaced),
                    valid_nodes=self._valid_nodes(cid),
                    after_effective_count=effective,
                    target_count=threshold,
                )
            if effective == 0:
                # 注册表仍持有原始提交样本，可从该样本恢复首份副本
                registry.content_data(cid)

            # 3) 按目标缺口补齐：need 以有效副本口径计算（含离线健康副本），
            #    因此无论修复期间节点如何上下线，修复后有效副本数都不会
            #    超出目标份数。被移除坏副本的节点若在线且容量足够，
            #    会作为普通候选被重新选中，即坏副本被原地替换。
            candidates = self._eligible_candidates(cid)
            chosen: list[str] = []
            need = threshold - effective
            for _ in range(max(0, need)):
                node_id = registry.place_on_first_eligible_node(cid, candidates)
                if node_id is None:
                    break
                chosen.append(node_id)

            after = registry.check_retrievability(cid)
            after_effective = registry.effective_replica_count(cid)
            valid_nodes = self._valid_nodes(cid)
            if not chosen and not replaced:
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=before_online,
                    after_online_count=after.online_replica_count,
                    reason="修复失败：没有合规且容量足够的在线节点可放置新副本",
                    valid_nodes=valid_nodes,
                    after_effective_count=after_effective,
                    target_count=threshold,
                )

            reason = self._describe(
                chosen=chosen,
                replaced=replaced,
                before_online=before_online,
                after_online=after.online_replica_count,
                after_effective=after_effective,
                threshold=threshold,
                retrievable=after.retrievable,
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
                after_effective_count=after_effective,
                target_count=threshold,
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

    @staticmethod
    def _describe(
        *,
        chosen: list[str],
        replaced: list[str],
        before_online: int,
        after_online: int,
        after_effective: int,
        threshold: int,
        retrievable: bool,
    ) -> str:
        actions: list[str] = []
        if chosen:
            actions.append(f"新增副本 {chosen}")
        if replaced:
            in_place = [n for n in replaced if n in chosen]
            if in_place:
                actions.append(
                    f"移除损坏副本所在节点 {replaced}（其中 {in_place} 已原地补回健康副本）"
                )
            else:
                actions.append(f"移除损坏副本所在节点 {replaced}")
        action_desc = "，".join(actions)
        counts = (
            f"在线副本数 {before_online} -> {after_online}，"
            f"有效副本数 {after_effective}/{threshold}（未超目标份数）"
        )
        if retrievable:
            return f"修复成功：{action_desc}，{counts}，达到阈值 {threshold}"
        if after_effective >= threshold:
            return (
                f"修复完成：{action_desc}，{counts}；"
                "有效副本已达目标份数，在线数待离线节点恢复后自然达标"
            )
        return (
            f"部分修复：{action_desc}，{counts}，"
            f"仍低于阈值 {threshold}（合规节点/容量不足）"
        )
