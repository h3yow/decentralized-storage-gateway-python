"""修复流程：按"有效副本口径"把健康副本补到目标份数，并替换已损坏副本。

补几份以"未损坏的有效副本（在线 + 暂时离线节点上的好副本）"相对目标份数
的缺口为准，而不是只看在线数，因此节点在修复期间上下线抖动不会让修复
结束后的有效副本数超出目标。
"""

from __future__ import annotations

from .errors import ContentNotFoundError
from .models import RepairReport
from .registry import ContentRegistry


class RepairManager:
    """根据当前注册表状态选择合规、在线、容量足够的节点补齐副本。

    修复目标口径：**有效副本数（未损坏，含暂时离线节点上的好副本）达到
    目标份数即可，且任何情况下都不超过目标份数**。判定是否需要补副本时，
    暂时离线但数据完好的副本仍然算数：它们一旦恢复就在目标之内，因此
    修复绝不能因为"此刻看不到它在线"而多补一份，否则节点抖动恢复后
    有效副本数会超出目标。

    修复与去重/放置/读取共享注册表锁，候选选择与实际放置为原子过程，
    因此并发修复不会对同一节点重复复制。修复的数据来源始终是注册表
    保存的可信样本或在线健康副本，从不读取任何已损坏副本，坏数据不会
    随修复扩散；健康好副本（即使离线）绝不删除、移动或覆盖。
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

            if result.retrievable and not corrupt_nodes:
                return self._report(
                    cid=cid,
                    repaired=False,
                    before_online=result.online_replica_count,
                    threshold=threshold,
                    reason=(
                        f"无需修复：在线副本数 {result.online_replica_count} "
                        f">= 阈值 {threshold}"
                    ),
                )

            # 1) 移除已判定损坏的副本：释放容量、清除损坏登记。
            #    坏副本不再计入任何计数，也不会被当作修复来源。
            replaced: list[str] = []
            for node_id in corrupt_nodes:
                registry.remove_replica(cid, node_id)
                replaced.append(node_id)

            # 2) 以"有效健康副本（在线 + 暂时离线的好副本）"作为修复口径
            #    重新计数。离线节点上的好副本数据仍然有效，必须计入目标，
            #    否则节点在修复期间下线、修复后恢复时有效副本数会超出目标。
            before_online = registry.online_replica_count(cid)
            healthy = registry.healthy_replica_count(cid)
            need = threshold - healthy

            chosen: list[str] = []
            deferred = False
            if need <= 0:
                # 有效副本（含暂时离线的好副本）已达到目标份数：
                # 哪怕当前在线数不足，本次也不再补新副本，绝不超量。
                pass
            elif before_online == 0 and healthy > 0:
                # 需要补副本，且离线节点上仍有健康好副本、但当前没有在线
                # 健康样本可复制：唯一样本原样保留在（离线）原节点，本次
                # 不补，等待节点恢复，绝不动用/覆盖那份好副本。
                deferred = True
            else:
                # healthy == 0：所有副本此前都已损坏并被移除，但注册表仍
                # 持有原始提交的可信样本，可从该样本把副本恢复到在线节点。
                # 3) 只在在线、合规、容量足够且尚无该副本的节点上补到目标。
                #    每次放置都在锁内实时筛选，修复期间节点抖动也只会让
                #    实际放置数 <= need，有效副本数最终不会超过目标份数。
                #    被移除坏副本的在线节点可作为普通候选被重新选中（原地替换）。
                candidates = self._eligible_candidates(cid)
                for _ in range(need):
                    node_id = registry.place_on_first_eligible_node(cid, candidates)
                    if node_id is None:
                        break
                    chosen.append(node_id)

            after = registry.check_retrievability(cid)
            valid_nodes = self._valid_nodes(cid)
            offline_valid = registry.offline_healthy_nodes_for(cid)
            reason = self._reason(
                threshold=threshold,
                replaced=replaced,
                chosen=chosen,
                before_online=before_online,
                after=after,
                need=need,
                deferred=deferred,
                offline_valid=offline_valid,
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
                target_replica_count=threshold,
                healthy_replica_count=after.healthy_replica_count,
                offline_valid_nodes=offline_valid,
            )

    def _report(
        self,
        *,
        cid: str,
        repaired: bool,
        before_online: int,
        threshold: int,
        reason: str,
    ) -> RepairReport:
        """构造修复报告（各计数统一从注册表快照读取，保证口径一致）。"""
        after = self._registry.check_retrievability(cid)
        return RepairReport(
            cid=cid,
            repaired=repaired,
            before_online_count=before_online,
            after_online_count=after.online_replica_count,
            reason=reason,
            replaced_corrupt_nodes=(),
            valid_nodes=self._valid_nodes(cid),
            target_replica_count=threshold,
            healthy_replica_count=after.healthy_replica_count,
            offline_valid_nodes=self._registry.offline_healthy_nodes_for(cid),
        )

    def _reason(
        self,
        *,
        threshold: int,
        replaced: list[str],
        chosen: list[str],
        before_online: int,
        after,
        need: int,
        deferred: bool,
        offline_valid: tuple[str, ...],
    ) -> str:
        actions: list[str] = []
        if chosen:
            actions.append(f"新增副本 {chosen}")
        if replaced:
            actions.append(f"替换损坏副本所在节点 {replaced}")
        action_desc = "，".join(actions) if actions else "未改动任何副本"
        offline_note = ""
        if offline_valid:
            offline_note = (
                f"；另有 {len(offline_valid)} 个有效副本位于离线节点 "
                f"{list(offline_valid)}，其恢复后有效副本数仍不超过目标"
            )

        if deferred:
            head = (
                "修复暂缓：需要补副本但全部有效副本所在节点离线，"
                "无在线健康样本可复制，唯一样本原样保留"
            )
            tail = f"，有效副本数 {after.healthy_replica_count}/{threshold}（未超目标）"
            if replaced:
                return f"{head}，已移除损坏副本 {replaced}{tail}"
            return f"{head}{tail}"

        if need <= 0:
            head = (
                f"无需补副本：有效副本数（含离线好副本）{after.healthy_replica_count} "
                f"已达到目标份数 {threshold}"
            )
            if replaced:
                head = f"已替换损坏副本 {replaced}，{head}"
            return (
                f"{head}，在线副本数 {before_online} -> {after.online_replica_count}"
                f"{offline_note}"
            )

        if after.retrievable:
            return (
                f"修复成功：{action_desc}，在线副本数 "
                f"{before_online} -> {after.online_replica_count}，达到阈值 {threshold}"
                f"{offline_note}"
            )
        if chosen or replaced:
            return (
                f"部分修复：{action_desc}，在线副本数 "
                f"{before_online} -> {after.online_replica_count}，有效副本数 "
                f"{after.healthy_replica_count}/{threshold}，"
                f"仍不足（合规节点/容量不足）{offline_note}"
            )
        return (
            "修复失败：没有合规且容量足够的在线节点可放置新副本，"
            f"有效副本数 {after.healthy_replica_count}/{threshold}"
            f"{offline_note}"
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
