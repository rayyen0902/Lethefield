"""exporter 聚合纯函数单测。"""

from datetime import UTC, datetime

from lethefield_metrics_exporter import aggregations


def _decision(outcome="accepted", escalation=None, has_agent=True, ts="2026-08-11T10:00:00Z"):
    return {
        "timestamp": ts,
        "event_type": "decision_recorded",
        "payload": {
            "outcome": outcome,
            "escalation_type": escalation,
            "has_agent_suggestion": has_agent,
        },
    }


def test_fold_counters_decision_rules():
    events = [
        _decision(outcome="rejected"),
        _decision(outcome="accepted"),
        _decision(outcome="accepted", has_agent=False),  # 纯人工决策不进分母
        _decision(escalation="cross_space"),
    ]
    folded = aggregations.fold_counters(events)
    assert folded["agent_suggestion_total"] == {"rejected": 1, "accepted": 2}
    assert folded["escalation_total"] == {"cross_space": 1}


def test_fold_counters_delta():
    events = [
        {
            "timestamp": "2026-08-11T10:00:00Z",
            "event_type": "ff_delta_applied",
            "payload": {"type": "neglect", "count": 5},
        },
        {
            "timestamp": "2026-08-11T10:01:00Z",
            "event_type": "ff_delta_applied",
            "payload": {"type": "conflict", "count": 1},
        },
        {
            "timestamp": "2026-08-11T10:02:00Z",
            "event_type": "decision_recorded",
            "payload": {
                "outcome": "accepted",
                "escalation_type": None,
                "has_agent_suggestion": False,
            },
        },
    ]
    folded = aggregations.fold_counters(events)
    assert folded["ff_delta_applied_total"] == {"neglect": 5, "conflict": 1}
    assert folded["agent_suggestion_total"] == {}


def _recall(space, keys, ts_ms):
    return {
        "timestamp": datetime.fromtimestamp(ts_ms / 1000, UTC).isoformat(),
        "event_type": "retrieve_recall_detail",
        "space_id": space,
        "payload": {"node_keys": keys, "stage_ms": {"knn": 1.0}},
    }


def _reinforce(space, key, ts_ms):
    return {
        "timestamp": datetime.fromtimestamp(ts_ms / 1000, UTC).isoformat(),
        "event_type": "memory_reinforced",
        "space_id": space,
        "payload": {"node_key": key},
    }


NOW = 1_800_000_000_000
W = 86_400_000


def test_recalled_then_touched_rate():
    recalls = [
        _recall("sp1", ["ev_1", "ev_2"], NOW - 1000),
        _recall("sp1", ["ev_3"], NOW - 2000),
        _recall("sp1", ["ev_old"], NOW - W - 10_000),  # 出窗不计
    ]
    reinforces = [
        _reinforce("sp1", "ev_1", NOW - 500),  # 窗内 touch
        _reinforce("sp1", "ev_2", NOW - W * 2),  # 召回之前的 touch 不算
    ]
    rate = aggregations.recalled_then_touched_rate(recalls, reinforces, window_ms=W, now_ms=NOW)
    assert rate == 1 / 3


def test_recalled_then_touched_rate_empty():
    assert aggregations.recalled_then_touched_rate([], [], window_ms=W, now_ms=NOW) is None


def test_lru_cache_hit_proxy():
    slow = {"knn": 600.0, "subgraph": 100.0, "ff_filter": 10.0}  # 总耗时超阈值
    fast = {"knn": 5.0}
    events = [
        {
            **_recall("sp1", ["a"], NOW - 10_000_000),
            "payload": {"node_keys": ["a"], "stage_ms": slow},
        },
        # 首次出现 + 高耗时 = 失效信号
        {
            **_recall("sp1", ["b"], NOW - 9_000_000),
            "payload": {"node_keys": ["b"], "stage_ms": slow},
        },
        # 间隔 1000s < idle(1800s) 不算闲置
        {
            **_recall("sp2", ["c"], NOW - 8_000_000),
            "payload": {"node_keys": ["c"], "stage_ms": fast},
        },
        # 闲置但不高耗时 = 不算失效
    ]
    proxy = aggregations.lru_cache_hit_proxy(events, idle_gap_ms=1_800_000, slow_stage_ms=500.0)
    assert proxy == 1.0 - 1 / 3


def test_lru_cache_hit_proxy_empty():
    assert aggregations.lru_cache_hit_proxy([], idle_gap_ms=1, slow_stage_ms=1) is None


def test_storage_bytes_by_tier():
    mappings = {"sp_a": "cold", "sp_b": "hot"}
    keyspace_bytes = {"sp_a": 100, "ex_sp_a": 50, "sp_b": 200, "ex_sp_b": 0, "stray": 999}
    counts = {"sp_a": 1, "sp_b": 3}
    out = aggregations.storage_bytes_by_tier(mappings, keyspace_bytes, counts, 1000)
    # ES 1000 字节按文档数 1:3 分摊：sp_a 250 / sp_b 750
    assert out == {"cold": 100 + 50 + 250, "hot": 200 + 750}
    assert 999 not in out.values()  # 无映射 keyspace 不进汇总
