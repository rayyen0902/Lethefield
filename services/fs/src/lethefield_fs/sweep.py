"""FS 周期 sweep 核心（M6，开发文档 §7 / 设计文档 §13.3、§13.5）。

单 space 一轮的四步判定（按序）：
1. 固化节点（has consolidated_at）整体跳过——s 锁定、跳过衰减与 sweep（M6 定案）；
2. 忽视惩罚：n_now − n_last_touched ≥ (neglect_count+1)×N_neglect → ff.apply_neglect
   （不更新 n_last_touched，否则惩罚自我抵消；区间幂等由 neglect_count 天然保证，
   同一忽视区间重复 sweep 不会产生重复惩罚）；
3. 固化判定先于归档：固化是对抗遗忘的保护（reinforce_count 达阈值且期间无 conflict）；
4. 归档判定：ff.archive_eligible（n_now ≥ n_star_cached + grace_n，宽限期为事件距离）；
5. n_star 顺带刷新：临近遗忘视界（未跨界、距离 ≤ margin）的节点重算 n_star_cached，
   重算值不同才写回（安全网；δ 路径本就立即重算）。

扫描范围是单 space 图内的事件节点（has('space_id', sid) 开头），非跨 space 全局扫描，
红线兼容。sweep 节奏只需显著快于 N_neglect 对应的事件推进速度，不追求实时。
"""

from dataclasses import dataclass

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_rms import ff

from lethefield_fs import archive as fs_archive
from lethefield_fs import consolidate as fs_consolidate
from lethefield_fs.config import DEFAULT_SWEEP_CONFIG, SweepConfig

# 一次取回 space 内全部事件节点的 φ 块（1.0 单节点规模从简；分页/批处理留待标定）
_FETCH_PHI_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_type', 'event')
    .project('node_key', 's', 'n', 'nstar', 'rc', 'cc', 'nc', 'consolidated')
    .by(values('node_key')).by(values('s')).by(values('n_last_touched'))
    .by(values('n_star_cached'))
    .by(values('reinforce_count')).by(values('conflict_count')).by(values('neglect_count'))
    .by(__.values('consolidated_at').fold())
    .toList()
['rows': rows]
"""

# n_star 顺带刷新落库：只写 n_star_cached 一个属性（衰减不物化纪律不受此影响——
# n_star_cached 是粗筛缓存，不是 s_effective）
_REFRESH_NSTAR_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def v = t.V().has('space_id', spaceId).has('node_key', nodeKey).next()
v.property('n_star_cached', nStar as long)
t.tx().commit()
'ok'
"""


@dataclass(frozen=True)
class NodePhi:
    """sweep 一轮取回的事件节点 φ 快照。"""

    node_key: str
    s: float
    n_last_touched: int
    n_star_cached: int
    reinforce_count: int
    conflict_count: int
    neglect_count: int
    consolidated: bool


@dataclass
class SweepStats:
    """单 space 一轮的处理计数（worker 聚合成指标，result 标签低基数枚举）。"""

    neglected: int = 0
    archived: int = 0
    consolidated: int = 0
    refreshed: int = 0
    skipped_consolidated: int = 0


# ---------------------------------------------------------------- 纯判定（可单测）

# neglect_due / consolidate_due 单点在 lethefield_rms.ff（M7 起，记忆动力学判定归
# FF 引擎，重放重建复用同一份）；此处 re-export 保持既有调用/测试导入不变。
neglect_due = ff.neglect_due
consolidate_due = ff.consolidate_due


def refresh_due(n_now: int, n_star_cached: int, margin: int) -> bool:
    """顺带刷新判定：临近遗忘视界（未跨界、距离 ≤ margin）。已跨界节点走归档判定。"""
    return 0 < n_star_cached - n_now <= margin


# ---------------------------------------------------------------- 单 space 一轮


def sweep_space(
    client: Client,
    cell_session: Session,
    es: Elasticsearch,
    *,
    gname: str,
    space_id: str,
    n_now: int,
    config: SweepConfig = DEFAULT_SWEEP_CONFIG,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> SweepStats:
    """对单个 space 执行一轮 sweep，返回处理计数。图名 = space_id（M5 定案约定）。"""
    payload = {
        k: v
        for item in client.submit(_FETCH_PHI_SCRIPT, {"gname": gname, "spaceId": space_id})
        .all()
        .result()
        for k, v in item.items()
    }
    rows = [
        NodePhi(
            node_key=row["node_key"],
            s=row["s"],
            n_last_touched=row["n"],
            n_star_cached=row["nstar"],
            reinforce_count=row["rc"],
            conflict_count=row["cc"],
            neglect_count=row["nc"],
            consolidated=bool(row["consolidated"]),
        )
        for row in payload.get("rows", [])
    ]

    stats = SweepStats()
    for node in rows:
        if node.consolidated:
            stats.skipped_consolidated += 1
            continue

        s, n_star, neglect_count = node.s, node.n_star_cached, node.neglect_count

        # 1. 忽视惩罚（区间幂等；不更新 n_last_touched）
        if neglect_due(n_now, node.n_last_touched, neglect_count, ff_config.n_neglect):
            phi = ff.apply_neglect(
                client,
                gname,
                space_id=space_id,
                node_key=node.node_key,
                n_now=n_now,
                config=ff_config,
            )
            stats.neglected += 1
            s, n_star, neglect_count = phi.s, phi.n_star_cached, phi.neglect_count

        # 2. 固化判定先于归档：达阈值且无 conflict 的记忆受固化保护，不因遗忘被清
        if consolidate_due(
            node.reinforce_count, node.conflict_count, config.consolidate_reinforce_threshold
        ):
            fs_consolidate.consolidate_node(
                client, gname, space_id=space_id, node_key=node.node_key
            )
            stats.consolidated += 1
            continue

        # 3. 归档判定（宽限期为事件距离 grace_n；固化节点 n_star=LONG_MAX 天然不满足）
        if ff.archive_eligible(n_now, n_star, ff_config.grace_n):
            fs_archive.archive_node(
                client,
                cell_session,
                es,
                gname=gname,
                space_id=space_id,
                node_key=node.node_key,
            )
            stats.archived += 1
            continue

        # 4. n_star 顺带刷新（安全网：重算值不同才写）
        if refresh_due(n_now, n_star, config.near_horizon_margin):
            recomputed = ff.n_star_horizon(
                s, node.n_last_touched, ff_config.theta_base, config=ff_config
            )
            if recomputed != n_star:
                result = (
                    client.submit(
                        _REFRESH_NSTAR_SCRIPT,
                        {
                            "gname": gname,
                            "spaceId": space_id,
                            "nodeKey": node.node_key,
                            "nStar": str(recomputed),
                        },
                    )
                    .all()
                    .result()
                )
                if "ok" not in result:
                    raise RuntimeError(f"n_star 刷新未返回 ok：{result}")
                stats.refreshed += 1

    return stats
