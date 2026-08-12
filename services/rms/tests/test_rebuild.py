"""rebuild 纯重放模型单测（M7）：不触存储，EX 事件流 → 重建计划的确定性重推。

覆盖：节点/temporal 边重建、reinforce 合并 count 展开、纠错 supersedes + −0.5、
链式纠错、忽视/固化/归档的理想化 sweep 重推、归档快照 M6 格式。
"""

from datetime import UTC, datetime, timedelta

import pytest
from lethefield_clients.ex_n import ExEvent, MetaEvent
from lethefield_rms import ff, rebuild
from lethefield_rms.rebuild import LONG_MAX, node_key_of, replay_events

_BASE = datetime(2026, 1, 1, tzinfo=UTC)


_UNSET = object()


def _event(n: int, ref_conflict: str | None = None, tau_ms=_UNSET) -> ExEvent:
    return ExEvent(
        n=n,
        event_id=f"e{n}",
        content=f"content-{n}",
        agent_actor_id="actor",
        account_id="acc",
        tau_ms=(n * 1000) if tau_ms is _UNSET else tau_ms,
        ref_conflict=ref_conflict,
        created_at=_BASE + timedelta(seconds=n),
    )


def _meta(node_key: str, *, count: int, n_at_event: int, at: int) -> MetaEvent:
    return MetaEvent(
        node_key=node_key,
        created_at=_BASE + timedelta(seconds=at),
        event_id=f"m-{node_key}-{at}",
        meta_type="reinforce",
        count=count,
        n_at_event=n_at_event,
        agent_actor_id="actor",
        account_id="acc",
    )


def _node(plan, event_n: int):
    key = node_key_of(f"e{event_n}")
    return next(n for n in plan.nodes if n.node_key == key)


def test_basic_chain_nodes_and_temporal_edges():
    plan = replay_events([_event(1), _event(2), _event(3)], [])
    assert [n.node_key for n in plan.nodes] == [node_key_of(f"e{i}") for i in (1, 2, 3)]
    assert plan.temporal_edges == [
        (node_key_of("e1"), node_key_of("e2")),
        (node_key_of("e2"), node_key_of("e3")),
    ]
    node = _node(plan, 1)
    assert node.s == rebuild.PLACEHOLDER_S  # 默认 s_resolver 占位常数
    assert node.n_last_touched == node.n_created == 1
    assert node.n_star_cached == ff.n_star_horizon(
        rebuild.PLACEHOLDER_S, 1, ff.DEFAULT_CONFIG.theta_base
    )
    assert (node.reinforce_count, node.conflict_count, node.neglect_count) == (0, 0, 0)
    assert plan.supersedes_edges == [] and plan.archives == []


def test_tau_falls_back_to_created_at():
    plan = replay_events([_event(1, tau_ms=None)], [])
    expected = int((_BASE + timedelta(seconds=1)).timestamp() * 1000)
    assert _node(plan, 1).tau_ms == expected


def test_reinforce_merged_count_expands():
    metas = [_meta(node_key_of("e1"), count=3, n_at_event=2, at=10)]
    plan = replay_events([_event(1), _event(2)], metas, s_resolver=lambda e: 0.5)
    node = _node(plan, 1)
    assert node.reinforce_count == 3  # 合并一笔 count=3 → 展开三次 +0.2
    assert node.s == pytest.approx(1.0)  # 0.5 + 0.6，upper 截断
    assert node.n_last_touched == 2  # touch 到 n_at_event


def test_conflict_builds_supersedes_and_soft_penalty():
    plan = replay_events([_event(1), _event(2, ref_conflict=node_key_of("e1"))], [])
    assert plan.supersedes_edges == [(node_key_of("e2"), node_key_of("e1"))]
    old = _node(plan, 1)
    assert old.s == pytest.approx(0.5)  # 1.0 − 0.5
    assert old.conflict_count == 1
    assert old.n_last_touched == 2  # conflict δ 更新 n_last_touched


def test_chain_correction_a_b_c():
    plan = replay_events(
        [
            _event(1),
            _event(2, ref_conflict=node_key_of("e1")),
            _event(3, ref_conflict=node_key_of("e2")),
        ],
        [],
    )
    assert plan.supersedes_edges == [
        (node_key_of("e2"), node_key_of("e1")),
        (node_key_of("e3"), node_key_of("e2")),
    ]
    assert _node(plan, 1).conflict_count == 1  # A 被 B 纠一次
    assert _node(plan, 2).conflict_count == 1  # B 被 C 纠一次（A→B 那笔不重复计）
    assert _node(plan, 3).conflict_count == 0


def test_neglect_replay_per_interval():
    events = [_event(n) for n in range(1, 42)]
    plan = replay_events(events, [])
    node = _node(plan, 1)
    n_neglect = ff.DEFAULT_CONFIG.n_neglect  # 20
    assert node.neglect_count == 2  # n=21 与 n=41 各一次（区间幂等序列）
    assert node.s == pytest.approx(1.0 - 2 * 0.1)
    assert node.n_last_touched == 1  # 忽视不更新 n_last_touched（否则惩罚自我抵消）
    assert n_neglect > 0


def test_consolidate_replay_locks_and_never_archives():
    metas = [_meta(node_key_of("e1"), count=3, n_at_event=1, at=1)]
    events = [_event(n) for n in range(1, 102)]
    plan = replay_events(events, metas)
    node = _node(plan, 1)
    assert node.consolidated is True  # reinforce_count=3 且无 conflict → 固化
    assert node.n_star_cached == LONG_MAX  # 固化 → 永不满足归档资格
    assert all(key != node.node_key for key, _ in plan.archives)


def test_conflict_on_consolidated_keeps_s_locked():
    metas = [_meta(node_key_of("e1"), count=3, n_at_event=1, at=1)]
    events = [_event(1), _event(2, ref_conflict=node_key_of("e1"))]
    plan = replay_events(events, metas)
    node = _node(plan, 1)
    assert node.consolidated is True
    assert node.s == pytest.approx(1.0)  # 固化后 −0.5 不改 s
    assert node.conflict_count == 1  # 计数器照计（计数是事实记录）
    assert plan.supersedes_edges == [(node_key_of("e2"), node_key_of("e1"))]  # 边照建


_NO_NEGLECT = ff.FFConfig(n_neglect=10_000)  # 归档测试隔离忽视干扰（窗口内不触发）


def test_archive_replay_with_m6_snapshot_format():
    events = [_event(n) for n in range(1, 46)]
    plan = replay_events(events, [], s_resolver=lambda e: 0.31, ff_config=_NO_NEGLECT)
    # s=0.31 → n*≈1 → n_star=n+1 → 归档于 n+41：e1..e4 归档，e5 起留在热图
    archived_keys = [key for key, _ in plan.archives]
    assert archived_keys == [node_key_of(f"e{i}") for i in (1, 2, 3, 4)]
    assert node_key_of("e1") not in [n.node_key for n in plan.nodes]
    assert node_key_of("e5") in [n.node_key for n in plan.nodes]

    key, snapshot = plan.archives[0]
    assert key == node_key_of("e1")
    props = snapshot["props"]
    assert props["content"] == "content-1"
    assert props["tau"] == 1000  # epoch 毫秒（M6 落 JSON 约定）
    assert props["ref_ex"] == "e1"
    assert props["s"] == pytest.approx(0.31)
    assert props["n_created"] == 1
    assert snapshot["edges"] == [
        {"label": "temporal", "out_key": node_key_of("e1"), "in_key": node_key_of("e2")}
    ]


def test_archive_snapshot_includes_supersedes_edges():
    # e2 纠 e1；e1 低 s 先归档，快照须含 supersedes 邻接（M6 快照 = 全字段 + 邻接）
    events = [_event(1), _event(2, ref_conflict=node_key_of("e1"))] + [
        _event(n) for n in range(3, 50)
    ]

    def resolver(e: ExEvent) -> float:
        return 0.31 if e.event_id == "e1" else 1.0

    plan = replay_events(events, [], s_resolver=resolver, ff_config=_NO_NEGLECT)
    archived = dict(plan.archives)
    snapshot = archived[node_key_of("e1")]
    labels = {(e["label"], e["out_key"], e["in_key"]) for e in snapshot["edges"]}
    assert ("supersedes", node_key_of("e2"), node_key_of("e1")) in labels
    assert ("temporal", node_key_of("e1"), node_key_of("e2")) in labels


def test_archive_snapshot_carries_vector_from_lookup():
    # M13 红线 3：归档快照携带原始 v_i——vector_lookup 注入取数，命中进快照、未命中 None
    events = [_event(n) for n in range(1, 46)]

    def lookup(node_key: str) -> list[float] | None:
        return [0.1, 0.2] if node_key == node_key_of("e1") else None

    plan = replay_events(
        events,
        [],
        s_resolver=lambda e: 0.31,
        ff_config=_NO_NEGLECT,
        vector_lookup=lookup,
    )
    archived = dict(plan.archives)
    assert archived[node_key_of("e1")]["v"] == [0.1, 0.2]
    assert archived[node_key_of("e2")]["v"] is None  # 查不到 → None（缺口由执行层登记）


def test_archive_snapshot_without_lookup_v_none():
    # 未注入 vector_lookup：v 恒 None，纯重放模型保持无 IO
    events = [_event(n) for n in range(1, 46)]
    plan = replay_events(events, [], s_resolver=lambda e: 0.31, ff_config=_NO_NEGLECT)
    assert plan.archives  # 前提：确实有归档发生
    assert all(snapshot["v"] is None for _, snapshot in plan.archives)
