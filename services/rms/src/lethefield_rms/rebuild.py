"""EX 重放重建（M7，开发文档 §8 验收项 2 / 设计文档 §13.7）：从 EX 事件流确定性重建 RMS。

重建范围（两档验收，v1.2 定案）：
- 重建：event 节点（含 φ 状态块）、temporal 边（写入链按时间序建立，n 序确定性可推）、
  supersedes 边 + conflict δ（EX `ref_conflict` 推导）、reinforce δ（EX 元事件 `count`
  展开）、忽视/固化/归档（理想化 sweep 确定性重推——复用 ff.neglect_due /
  ff.consolidate_due / ff.archive_eligible 单点判定，禁止抄副本）。
- **不重建**：semantic/causal/entity 边（consolidation 推断产物，EX 无记录，
  M14/M15 落地后才有来源）；节点初始 `s` 走注入 `s_resolver`——M14 前默认占位常数
  （验收不含 s 保真），M14 后切换为读 EX `scoring_result` 元事件（全保真）。
- `consolidated_at` 时间戳不可复现：重放置重放时刻——**存在性保真、时间戳不保真**
  （固化语义只在"存在即锁定"，时间戳仅审计用途）。

理想化 sweep 语义（确定性来源）：重放假定 sweep 在每个事件推进点都执行——
连续 sweep 下节点跨 k 个完整忽视区间恰受 k 次惩罚（区间幂等判定天然给出此序列）。
真实 sweep 节奏不可复现，本语义是可接受设计（重建 = 规范历史，不是逐拍复刻）。

过渡约定（与 s_resolver 同款模式，M15 冻结后切换）：
- `node_key_of(event_id)`：EX 不存 node_key（M15 写入链才定生成规则），重建节点键
  由本单点函数生成；M15 落地后此函数对齐其规则，调用方零改动。
- `consolidate_threshold` 默认 3，与 services/fs SweepConfig 占位保持一致（服务边界
  禁止互 import，配置值经参数对齐、标定流程统一调整）。
"""

import argparse
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from cassandra.cluster import Session
from gremlin_python.driver.client import Client
from lethefield_clients import (
    cassandra_cluster,
    ensure_archive_table,
    ex_cassandra_cluster,
    gremlin_client,
    list_experience_events,
    list_meta_events,
    write_archive,
)
from lethefield_clients.ex_n import ExEvent, MetaEvent

from lethefield_rms import ff, writer
from lethefield_rms.schema import ensure_graph_schema

# Java Long 上限（与 ff._LONG_MAX / fs.consolidate.LONG_MAX 同值）：固化节点绝对遗忘视界 = +∞
LONG_MAX = 2**63 - 1

# 初始 s 占位常数（M14 前；打分可重建性定案见开发文档 §8 验收项 2 两档说明）
PLACEHOLDER_S = 1.0

# 固化阈值占位（与 services/fs SweepConfig.consolidate_reinforce_threshold 对齐）
DEFAULT_CONSOLIDATE_THRESHOLD = 3

# 固化锁定哨兵：compute_delta 以 consolidated_at 非空判定锁定，重放模型用哨兵复用同一纯函数
_CONSOLIDATED_SENTINEL = datetime(1970, 1, 1, tzinfo=UTC)


def node_key_of(event_id: str) -> str:
    """重建节点键生成单点（过渡约定，M15 冻结 node_key 规则后对齐切换）。"""
    return f"ev_{event_id}"


def placeholder_s_resolver(event: ExEvent) -> float:
    """默认 s_resolver：占位常数（M14 落地后切换为读 EX scoring_result 元事件）。"""
    return PLACEHOLDER_S


# ---------------------------------------------------------------- 纯重放模型（不触存储，可单测）


@dataclass
class ReplayedNode:
    """重放中的节点状态（δ 调整一律经 ff.compute_delta 纯函数，与生产路径同一公式）。"""

    node_key: str
    content: str
    tau_ms: int
    ref_ex: str
    agent_actor_id: str | None
    n_created: int
    s: float
    n_last_touched: int
    n_star_cached: int
    reinforce_count: int = 0
    conflict_count: int = 0
    neglect_count: int = 0
    consolidated: bool = False

    def phi(self) -> ff.PhiState:
        return ff.PhiState(
            s=self.s,
            n_last_touched=self.n_last_touched,
            n_star_cached=self.n_star_cached,
            reinforce_count=self.reinforce_count,
            conflict_count=self.conflict_count,
            neglect_count=self.neglect_count,
            consolidated_at=_CONSOLIDATED_SENTINEL if self.consolidated else None,
        )

    def apply(self, new: ff.PhiState) -> None:
        self.s = new.s
        self.n_last_touched = new.n_last_touched
        self.n_star_cached = new.n_star_cached
        self.reinforce_count = new.reinforce_count
        self.conflict_count = new.conflict_count
        self.neglect_count = new.neglect_count


@dataclass(frozen=True)
class ReplayPlan:
    """重放产物：执行器据此写目标图 + archived_nodes。"""

    nodes: list[ReplayedNode]  # 热图节点（未归档，n 序）
    temporal_edges: list[tuple[str, str]]  # (from_key, to_key)
    supersedes_edges: list[tuple[str, str]]  # (new_key, old_key)
    archives: list[tuple[str, dict]]  # (node_key, M6 格式快照 {"props":…, "edges":…})


def _epoch_ms(dt: datetime) -> int:
    """Cassandra timestamp → epoch 毫秒（naive 按 UTC 解释，M6 踩坑定案）。"""
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return int(aware.timestamp() * 1000)


def replay_events(
    events: list[ExEvent],
    metas: list[MetaEvent],
    *,
    s_resolver: Callable[[ExEvent], float] = placeholder_s_resolver,
    consolidate_threshold: int = DEFAULT_CONSOLIDATE_THRESHOLD,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> ReplayPlan:
    """纯重放：EX 事件流 → 重建计划。输入 events 按 n 升序（list_experience_events 保证）。"""
    live: dict[str, ReplayedNode] = {}
    order: list[str] = []  # 热图节点 n 序（归档即移除）
    temporal_edges: list[tuple[str, str]] = []
    supersedes_edges: list[tuple[str, str]] = []
    superseded_pairs: set[tuple[str, str]] = set()
    archives: list[tuple[str, dict]] = []

    # 元事件按 (n_at_event, created_at) 排序待施加；窗口合并后一笔的 n_at_event 是
    # 窗口内最后一次强化的 n——重建精度即窗口粒度（可接受设计，开发文档 §8）
    pending = sorted(metas, key=lambda m: (m.n_at_event or 0, m.created_at))

    def apply_delta(
        node: ReplayedNode, *, delta: float, touch: bool, counter_key: str, n: int
    ) -> None:
        node.apply(
            ff.compute_delta(
                node.phi(),
                delta=delta,
                touch=touch,
                counter_key=counter_key,
                n_now=n,
                config=ff_config,
            )
        )

    def apply_metas_up_to(n: int) -> None:
        nonlocal pending
        rest = []
        for meta in pending:
            if meta.meta_type != "reinforce":
                rest.append(meta)  # 未来类型（scoring_result 等）不在重放范围，不消费
                continue
            if meta.n_at_event is not None and meta.n_at_event <= n:
                node = live.get(meta.node_key)
                if node is not None:  # 节点已归档则强化无处施加（生产路径同：节点不存在）
                    for _ in range(meta.count):
                        apply_delta(
                            node,
                            delta=ff.DELTA_REINFORCE,
                            touch=True,
                            counter_key="reinforce_count",
                            n=meta.n_at_event,
                        )
            else:
                rest.append(meta)
        pending = rest

    def snapshot_of(node: ReplayedNode) -> dict:
        """归档快照（M6 build_snapshot 同格式：tau 等时间戳落 epoch 毫秒）。

        邻接只含对端仍在热图的边——顶点归档即携边移除，先归档邻居的悬挂边
        与热图状态一致地不出现在快照里。
        """
        props: dict = {
            "content": node.content,
            "tau": node.tau_ms,
            "ref_ex": node.ref_ex,
            "s": node.s,
            "n_created": node.n_created,
            "n_last_touched": node.n_last_touched,
            "n_star_cached": node.n_star_cached,
            "reinforce_count": node.reinforce_count,
            "conflict_count": node.conflict_count,
            "neglect_count": node.neglect_count,
        }
        if node.agent_actor_id is not None:
            props["agent_actor_id"] = node.agent_actor_id
        edges = [
            {"label": label, "out_key": out_key, "in_key": in_key}
            for label, pairs in (("temporal", temporal_edges), ("supersedes", supersedes_edges))
            for out_key, in_key in pairs
            if node.node_key in (out_key, in_key) and out_key in live and in_key in live
        ]
        return {"props": props, "edges": edges}

    def sweep_at(n: int) -> None:
        """理想化 sweep（每个事件推进点执行）：忽视 → 固化（先于归档）→ 归档。"""
        for key in list(order):
            node = live.get(key)
            if node is None or node.consolidated:
                continue
            if ff.neglect_due(n, node.n_last_touched, node.neglect_count, ff_config.n_neglect):
                apply_delta(
                    node,
                    delta=ff.DELTA_NEGLECT,
                    touch=False,
                    counter_key="neglect_count",
                    n=n,
                )
            if ff.consolidate_due(node.reinforce_count, node.conflict_count, consolidate_threshold):
                node.consolidated = True  # consolidated_at 时间戳不可复现，重放置执行时刻
                node.n_star_cached = LONG_MAX
                continue
            if ff.archive_eligible(n, node.n_star_cached, ff_config.grace_n):
                archives.append((key, snapshot_of(node)))
                del live[key]
                order.remove(key)

    for event in events:
        key = node_key_of(event.event_id)
        s = s_resolver(event)
        node = ReplayedNode(
            node_key=key,
            content=event.content,
            tau_ms=event.tau_ms if event.tau_ms is not None else _epoch_ms(event.created_at),
            ref_ex=event.event_id,
            agent_actor_id=event.agent_actor_id,
            n_created=event.n,
            s=s,
            n_last_touched=event.n,
            n_star_cached=ff.n_star_horizon(s, event.n, ff_config.theta_base, config=ff_config),
        )
        live[key] = node
        # temporal 边按 n 序链：前一节点仍在热图才建（已归档端点的悬挂边情形归 M15 写入链定）
        if order:
            temporal_edges.append((order[-1], key))
        order.append(key)

        # 纠错事件：supersedes 边 + 旧节点 −0.5（同一对节点幂等，与 corrections.py 同语义）
        if event.ref_conflict:
            old_key = event.ref_conflict
            pair = (key, old_key)
            old = live.get(old_key)
            if pair not in superseded_pairs and old is not None:
                supersedes_edges.append(pair)
                superseded_pairs.add(pair)
                apply_delta(
                    old,
                    delta=ff.DELTA_CONFLICT,
                    touch=True,
                    counter_key="conflict_count",
                    n=event.n,
                )

        apply_metas_up_to(event.n)
        sweep_at(event.n)

    # 尾批：n_at_event 不超过最终 n 的剩余元事件 + 末态 sweep
    final_n = events[-1].n if events else 0
    apply_metas_up_to(final_n)
    if events:
        sweep_at(final_n)

    return ReplayPlan(
        nodes=[live[key] for key in order],
        temporal_edges=temporal_edges,
        supersedes_edges=supersedes_edges,
        archives=archives,
    )


# ------------------------------------------------------ 执行器（计划 → 目标图 + archived_nodes）

# 重建建点脚本：最终 φ 全字段一次落库（重建是恢复工具，不走 writer 的 φ 初始化路径——
# writer 初始化约束服务生产写入链，重建写入的是重放算出的终态）。
_CREATE_NODE_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def v = t.addV()
    .property('node_key', nodeKey)
    .property('space_id', spaceId)
    .property('node_type', 'event')
    .property('content', contentText)
    .property('tau', new Date(tauMs as long))
    .property('ref_ex', refEx)
    .property('s', sVal as double)
    .property('n_created', nCreated as long)
    .property('n_last_touched', nTouched as long)
    .property('n_star_cached', nStar as long)
    .property('reinforce_count', rc as int)
    .property('conflict_count', cc as int)
    .property('neglect_count', nc as int)
    .next()
if (actorId != null) { v.property('agent_actor_id', actorId) }
if (consolidatedFlag) { v.property('consolidated_at', new Date(nowMs as long)) }
t.tx().commit()
'ok'
"""


def execute_rebuild(
    client: Client,
    cell_session: Session,
    plan: ReplayPlan,
    *,
    target_gname: str,
    space_id: str,
) -> None:
    """把重建计划落到目标图 + 该图 keyspace 的 archived_nodes 表。"""
    now_ms = int(time.time() * 1000)
    for node in plan.nodes:
        result = (
            client.submit(
                _CREATE_NODE_SCRIPT,
                {
                    "gname": target_gname,
                    "nodeKey": node.node_key,
                    "spaceId": space_id,
                    "contentText": node.content,
                    "tauMs": str(node.tau_ms),
                    "refEx": node.ref_ex,
                    "sVal": node.s,
                    "nCreated": str(node.n_created),
                    "nTouched": str(node.n_last_touched),
                    "nStar": str(node.n_star_cached),
                    "rc": node.reinforce_count,
                    "cc": node.conflict_count,
                    "nc": node.neglect_count,
                    "actorId": node.agent_actor_id,
                    "consolidatedFlag": node.consolidated,
                    "nowMs": str(now_ms),
                },
            )
            .all()
            .result()
        )
        if "ok" not in result:
            raise RuntimeError(f"重建建点未返回 ok：{result}")
    # 归档节点不进热图：只建双端都在热图的边（顶点不存在 addEdge 会失败，
    # 且与真实图语义一致——顶点归档即携边移除）
    live_keys = {n.node_key for n in plan.nodes}
    for from_key, to_key in plan.temporal_edges:
        if from_key in live_keys and to_key in live_keys:
            writer.create_edge(
                client,
                target_gname,
                space_id=space_id,
                from_key=from_key,
                to_key=to_key,
                label="temporal",
            )
    for new_key, old_key in plan.supersedes_edges:
        if new_key in live_keys and old_key in live_keys:
            writer.create_edge(
                client,
                target_gname,
                space_id=space_id,
                from_key=new_key,
                to_key=old_key,
                label="supersedes",
            )
    if plan.archives:
        ensure_archive_table(cell_session, target_gname)
        for node_key, snapshot in plan.archives:
            write_archive(cell_session, target_gname, node_key=node_key, snapshot=snapshot)


def rebuild_space(
    client: Client,
    cell_session: Session,
    ex_session: Session,
    *,
    space_id: str,
    target_gname: str,
    s_resolver: Callable[[ExEvent], float] = placeholder_s_resolver,
    consolidate_threshold: int = DEFAULT_CONSOLIDATE_THRESHOLD,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> ReplayPlan:
    """端到端：读 EX → 重放 → 目标图建 schema → 落计划。返回计划供校验比对。"""
    events = list_experience_events(ex_session, space_id=space_id)
    metas = list_meta_events(ex_session, space_id=space_id)
    plan = replay_events(
        events,
        metas,
        s_resolver=s_resolver,
        consolidate_threshold=consolidate_threshold,
        ff_config=ff_config,
    )
    ensure_graph_schema(client, target_gname)
    execute_rebuild(client, cell_session, plan, target_gname=target_gname, space_id=space_id)
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(
        description="EX 重放重建（M7）：从 EX 事件流确定性重建 RMS 图结构与归档表"
    )
    parser.add_argument("space_id", help="源 space（读 ex_<space_id>）")
    parser.add_argument(
        "--target-gname",
        help="目标图名（缺省 = space_id，原地重建；混沌演练'删了重建'显式给新图名）",
    )
    args = parser.parse_args()

    client = gremlin_client()
    cell_cluster = cassandra_cluster()
    cell_session = cell_cluster.connect()
    ex_cluster = ex_cassandra_cluster()
    ex_session = ex_cluster.connect()
    try:
        plan = rebuild_space(
            client,
            cell_session,
            ex_session,
            space_id=args.space_id,
            target_gname=args.target_gname or args.space_id,
        )
    finally:
        client.close()
        cell_cluster.shutdown()
        ex_cluster.shutdown()
    print(
        f"[ok] space={args.space_id} → 图 {args.target_gname or args.space_id}："
        f"节点 {len(plan.nodes)}、temporal 边 {len(plan.temporal_edges)}、"
        f"supersedes 边 {len(plan.supersedes_edges)}、归档 {len(plan.archives)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
