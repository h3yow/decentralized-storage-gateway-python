"""修复流程：替换在线坏副本、把在线健康副本补齐到阈值。"""

from __future__ import annotations

from .models import NodeStatus, RepairReport
from .registry import ContentRegistry


class RepairManager:
    """依据当前注册表状态修复单个内容的可检索性。

    修复分两类动作，且判定与动作整体在注册表锁内串行完成：

    1. **替换坏副本**：在线但字节与 CID 不符的副本，用注册表保存的可信
       样本就地重写。替换不新增副本、不改变容量记账，坏数据不会被当成
       修复来源，也不会扩散；
    2. **补齐缺口**：替换后若在线健康副本仍不足阈值，再向合规、在线、
       容量足够、尚无该副本的节点新增副本，**只补到阈值为止**，
       因此修复结束后健康有效副本数绝不会超过修复目标。

    唯一健康样本（注册表可信样本 + 现有健康节点副本）从不删除或移动。
    """

    def __init__(self, registry: ContentRegistry) -> None:
        self._registry = registry

    def repair(self, cid: str) -> RepairReport:
        registry = self._registry
        # 判定 + 替换 + 补齐全部在注册表锁的串行化保护下完成，
        # 并发修复/读取/上下线彼此不会观察到中间态。
        with registry.lock():
            result = registry.check_retrievability(cid)

            # 先处理在线坏副本：无论当前是否达标，都尝试用可信样本替换，
            # 使"哪些节点上是有效副本、坏副本是否被换掉"在报告中可解释。
            replaced: list[str] = []
            for node in registry.list_nodes():
                if (
                    node.status == NodeStatus.ONLINE
                    and node.node_id in registry.corrupted_nodes(cid)
                ):
                    registry.replace_corrupted_replica(cid, node.node_id)
                    replaced.append(node.node_id)
            replaced.sort()

            after_replace = registry.check_retrievability(cid)
            before_online = result.online_replica_count

            # 全部健康副本都不在线：没有在线健康样本可复制时，不盲目动手
            # （注册表可信样本仍可用于"零副本"冷启动，见下方补齐逻辑）。
            if (
                after_replace.online_replica_count == 0
                and after_replace.total_replica_count > 0
                and after_replace.online_corrupted_count == 0
            ):
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=before_online,
                    after_online_count=0,
                    replaced_nodes=tuple(replaced),
                    valid_replica_count=registry.healthy_replica_count(cid),
                    reason="修复失败：全部健康副本所在节点离线，无在线健康样本可复制",
                )

            # 补齐数量同时受两个上界约束，取较小值，杜绝任何形式的超标：
            # 1) 在线缺口 threshold - 在线健康数（恢复可检索性所需）；
            # 2) 阈值总余量 threshold - 全部健康副本数（含离线健康副本）。
            #    节点抖动时离线健康副本将来会恢复，绝不为它们先补占位副本，
            #    否则离线节点一回来有效副本数就会超过修复目标。
            online_gap = result.threshold - after_replace.online_replica_count
            headroom = result.threshold - registry.healthy_replica_count(cid)
            need = min(online_gap, headroom)
            candidates = self._eligible_candidates(cid)
            chosen: list[str] = []
            # 只补到阈值：need<=0 时不新增任何副本，有效副本数绝不超标
            for _ in range(max(0, need)):
                node_id = registry.place_on_first_eligible_node(cid, candidates)
                if node_id is None:
                    break
                chosen.append(node_id)

            after = registry.check_retrievability(cid)
            valid_total = registry.healthy_replica_count(cid)

            if not chosen and not replaced:
                if after.retrievable:
                    return RepairReport(
                        cid=cid,
                        repaired=False,
                        before_online_count=before_online,
                        after_online_count=after.online_replica_count,
                        target_nodes=(),
                        replaced_nodes=(),
                        valid_replica_count=valid_total,
                        reason=(
                            "无需修复：在线健康副本数 "
                            f"{after.online_replica_count} >= 阈值 {after.threshold}"
                        ),
                    )
                if headroom <= 0:
                    reason = (
                        "修复受限：有效健康副本总数已达阈值 "
                        f"{result.threshold}（部分健康副本当前在离线节点上），"
                        "为避免超标不新增副本，等待离线节点恢复"
                    )
                else:
                    reason = "修复失败：没有合规且容量足够的在线节点可放置新副本"
                return RepairReport(
                    cid=cid,
                    repaired=False,
                    before_online_count=before_online,
                    after_online_count=after.online_replica_count,
                    target_nodes=(),
                    replaced_nodes=(),
                    valid_replica_count=valid_total,
                    reason=reason,
                )

            parts: list[str] = []
            if replaced:
                parts.append(f"就地替换坏副本 {replaced}（不新增副本、容量记账不变）")
            if chosen:
                parts.append(
                    f"新增副本 {chosen}，在线健康副本数 "
                    f"{before_online} -> {after.online_replica_count}"
                )
            else:
                parts.append(f"在线健康副本数维持 {after.online_replica_count}")
            parts.append(
                f"当前有效副本 {valid_total} 个（修复目标阈值 {after.threshold}）"
            )
            if after.retrievable:
                reason = "修复成功：" + "；".join(parts)
            else:
                reason = (
                    "部分修复：" + "；".join(parts)
                    + f"；仍低于阈值 {after.threshold}（合规节点/容量不足）"
                )
            return RepairReport(
                cid=cid,
                repaired=after.retrievable,
                before_online_count=before_online,
                after_online_count=after.online_replica_count,
                target_nodes=tuple(chosen),
                reason=reason,
                replaced_nodes=tuple(replaced),
                valid_replica_count=valid_total,
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
