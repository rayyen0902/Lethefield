"""聚合纯函数（不触存储，可单测）。输入 = 日志事件字典序列 / 元数据字典，输出 = 指标值。

所有口径的诚实注释都在这里——近似口径改代码前先把注释读一遍。
"""

from datetime import UTC, datetime
from typing import Any


def _ts_ms(event: dict[str, Any]) -> int:
    ts = event["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return int(ts.timestamp() * 1000)


def fold_counters(events: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """留痕线/δ 计数折叠：{metric: {label_value: count}}。

    - decision_recorded：has_agent_suggestion 才计 agent_suggestion_total{outcome}
      （纯人工决策不稀释分母，§19.4 语义）；escalation_type 非空计 escalation_total{reason}。
    - ff_delta_applied：按 payload count 累加 ff_delta_applied_total{type}。
    """
    out: dict[str, dict[str, int]] = {
        "agent_suggestion_total": {},
        "escalation_total": {},
        "ff_delta_applied_total": {},
    }

    def bump(metric: str, label: str, n: int = 1) -> None:
        out[metric][label] = out[metric].get(label, 0) + n

    for e in events:
        et = e.get("event_type")
        payload = e.get("payload", {})
        if et == "decision_recorded":
            if payload.get("has_agent_suggestion"):
                bump("agent_suggestion_total", payload["outcome"])
            if payload.get("escalation_type"):
                bump("escalation_total", payload["escalation_type"])
        elif et == "ff_delta_applied":
            bump("ff_delta_applied_total", payload["type"], int(payload.get("count", 1)))
    return out


def recalled_then_touched_rate(
    recall_events: list[dict[str, Any]],
    reinforce_events: list[dict[str, Any]],
    *,
    window_ms: int,
    now_ms: int,
) -> float | None:
    """近 window 内被召回节点在召回后 window 内被 reinforce 的比例（λ 核心验收，§19.2）。

    关联键 (space_id, node_key)（召回明细 LogEvent 的 space_id 字段 × node_keys 列表）。
    窗口右缘附近的召回未满观察期——开发期口径接受此近似（趋势判读不受影响）。
    无召回返回 None（调用方不更新 gauge）。
    """
    recalls: list[tuple[str, str, int]] = []
    for e in recall_events:
        ts = _ts_ms(e)
        if now_ms - ts > window_ms:
            continue
        for node_key in e.get("payload", {}).get("node_keys", []):
            recalls.append((e["space_id"], node_key, ts))
    if not recalls:
        return None
    touches: set[tuple[str, str, int]] = set()
    for e in reinforce_events:
        touches.add((e["space_id"], e.get("payload", {}).get("node_key"), _ts_ms(e)))
    touched = 0
    for space_id, node_key, recall_ts in recalls:
        if any(
            s == space_id and k == node_key and recall_ts <= t <= recall_ts + window_ms
            for s, k, t in touches
        ):
            touched += 1
    return touched / len(recalls)


def lru_cache_hit_proxy(
    recall_events: list[dict[str, Any]],
    *,
    idle_gap_ms: int,
    slow_stage_ms: float,
) -> float | None:
    """graph_lru_cache_hit_ratio 的离线代理（M12 升级定案口径）。

    缓存失效信号 = 同 space 距上次召回超 idle_gap_ms（或首次出现）且 Stage 总耗时
    > slow_stage_ms 的请求；返回值 = 1 − 失效信号占比。回答的是"缓存失效频率"——
    LRU 容量/预热标定的真实问题；不是服务端命中率的直接测量（客户端观测不到，
    定案否决 JMX 通道）。无召回事件返回 None。
    """
    events = sorted((e for e in recall_events if e.get("space_id")), key=_ts_ms)
    if not events:
        return None
    last_seen: dict[str, int] = {}
    miss_signals = 0
    for e in events:
        ts = _ts_ms(e)
        stage = e.get("payload", {}).get("stage_ms") or {}
        total_ms = sum(float(v) for v in stage.values())
        idle = ts - last_seen.get(e["space_id"], ts - idle_gap_ms - 1) > idle_gap_ms
        last_seen[e["space_id"]] = ts
        if idle and total_ms > slow_stage_ms:
            miss_signals += 1
    return 1.0 - miss_signals / len(events)


def storage_bytes_by_tier(
    mappings: dict[str, str],  # space_id -> tier
    keyspace_bytes: dict[str, int],  # keyspace -> bytes（cell 图 + EX 两集群合并视图）
    vector_doc_counts: dict[str, int],  # space_id -> rms_vectors 文档数
    vector_total_bytes: int,
) -> dict[str, int]:
    """space_storage_bytes{tier} 聚合：{tier: bytes}。

    口径：RMS 图 keyspace（= space_id）+ EX keyspace（ex_{space}）直加；
    rms_vectors 共享索引按文档数比例分摊（ES 不提供 per-routing 字节数——近似口径，
    成本验证（§15.3）用途下足够）。未开通映射的 keyspace 不进汇总（控制面为准）。
    """
    total_docs = sum(vector_doc_counts.values())
    out: dict[str, int] = {}
    for space_id, tier in mappings.items():
        nbytes = keyspace_bytes.get(space_id, 0) + keyspace_bytes.get(f"ex_{space_id}", 0)
        if total_docs and vector_total_bytes:
            nbytes += int(vector_total_bytes * vector_doc_counts.get(space_id, 0) / total_docs)
        out[tier] = out.get(tier, 0) + nbytes
    return out
