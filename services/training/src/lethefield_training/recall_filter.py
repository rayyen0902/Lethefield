"""③ 入料口生产侧过滤器（M12 收口定案形态）：读 es-ops 日志管线 → 授权闸门 → 训练 topic。

替代 M11 过渡形态（API 进程内直发）——训练管线职责（注册表查询、topic 发布）
不塞请求路径服务，与"埋点物理分开"原则对齐。

纪律（升级定案三条执行要求）：
1. at-least-once + checkpoint（state 文件游标）；召回明细事件带唯一 event_id，
   worker 侧按 ID 去重（重放不虚增 R3 关联基数）。
2. 授权拦截点不变：**入 topic 前**查注册表（CALIBRATION scope）；worker 侧复查
   保留为第二道防线。
3. 只读 es-ops 日志管线（运维基础设施），不触业务库（红线 1）。
"""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lethefield_clients import (
    AuthRegistryStore,
    AuthScope,
    FeedEvent,
    FeedKind,
    FeedSource,
    redline1_exempt,
)

from lethefield_training.worker import FEED_DROPPED_TOTAL

# 过滤的事件类型（API retrieve 发射的召回明细）
RECALL_EVENT_TYPE = "retrieve_recall_detail"


def _load_cursor(state_path: Path) -> list | None:
    if not state_path.exists():
        return None
    return json.loads(state_path.read_text(encoding="utf-8")).get("search_after")


def _save_cursor(state_path: Path, cursor: list) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({"search_after": cursor}), encoding="utf-8")


def fetch_recall_events(es_ops, cursor: list | None) -> tuple[list[dict[str, Any]], list | None]:
    """按 (timestamp, _id) 游标读新增召回明细事件（单轮最多 1000 条，下轮续）。"""
    query: dict[str, Any] = {
        "size": 1000,
        "sort": [
            {"timestamp": "asc"}
        ],  # _id 不可排序（fielddata 限制）；同毫秒边界重读由 worker event_id 去重兜底
        "query": {"term": {"event_type": RECALL_EVENT_TYPE}},
    }
    if cursor:
        query["search_after"] = cursor
    resp = es_ops.search(index="lethefield-logs-*", body=query)
    hits = resp["hits"]["hits"]
    events = [hit["_source"] for hit in hits]
    new_cursor = list(hits[-1]["sort"]) if hits else cursor
    return events, new_cursor


def to_feed_event(event: dict[str, Any]) -> FeedEvent:
    """召回明细日志事件 → feed 信封（原样保留 event_id/stage_ms 与原始时间戳）。"""
    payload = event["payload"]
    if not payload.get("event_id"):
        raise ValueError("召回明细缺 event_id（fail-closed，不放行无去重键的事件）")
    ts = event["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return FeedEvent(
        kind=FeedKind.RECALL_DETAIL,
        source=FeedSource.FF_METRIC,
        space_ref=payload["space_ref"],
        payload=payload,
        emitted_at=ts,
    )


@redline1_exempt(
    worker="training-recall-filter",
    reason=(
        "只读 es-ops 运维日志管线（非业务存储，无 space 枚举）；逐事件过 CALIBRATION "
        "授权闸门后转发；at-least-once + checkpoint 游标推进，批间节流 = 轮询间隔"
    ),
    cadence="--interval 轮询（默认 5s）",
)
def run_once(
    es_ops,
    *,
    registry: AuthRegistryStore,
    publish: Callable[[FeedEvent], None],
    state_path: str | Path,
    emit: Callable[[Any], None] | None = None,
) -> int:
    """单轮：读新增召回明细 → 授权闸门 → 转发训练 topic → 推进 checkpoint。返回转发条数。"""
    state_path = Path(state_path)
    events, new_cursor = fetch_recall_events(es_ops, _load_cursor(state_path))
    forwarded = 0
    for event in events:
        feed = to_feed_event(event)  # schema 不符抛 ValueError（游标不推进，下轮重试）
        if not registry.is_authorized(feed.space_ref or "", AuthScope.CALIBRATION):
            # 未授权 space 的 ③ 类数据在入 topic 前拦截（既定拦截点）
            FEED_DROPPED_TOTAL.labels(kind=str(feed.kind), reason="unauthorized").inc()
            continue
        publish(feed)
        forwarded += 1
    if new_cursor:
        _save_cursor(state_path, new_cursor)
    return forwarded
