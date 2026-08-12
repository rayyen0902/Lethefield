"""exporter 主循环：日志流聚合 + 元数据采集 → 指标注册表（/metrics 由 __main__ 暴露）。

数据源边界（红线 1 / §13 验收"聚合不扫业务存储"）：
- es-ops 日志管线（lethefield-logs-*）：留痕线/δ counter、touched rate、lru 代理；
- Cassandra `system.size_estimates`（cell + EX 两集群，系统表非业务数据）；
- 控制面映射表（spaces/cells，元数据）；ES rms_vectors 仅 _stats/_count 元数据口径。

counter 重启重建：进程启动从头折叠 es-ops 全量事件（开发期日志量有界——
es-ops 滚动清理后规模问题再议，届时给 counter 加状态持久化）。
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from lethefield_clients import redline1_exempt
from lethefield_metrics import counter, gauge, histogram
from prometheus_client import REGISTRY

from lethefield_metrics_exporter import aggregations
from lethefield_metrics_exporter.config import DEFAULT_CONFIG, ExporterConfig

# 关心的日志事件类型（es-ops 轮询过滤）
TRACKED_EVENT_TYPES = (
    "decision_recorded",
    "ff_delta_applied",
    "retrieve_recall_detail",
    "memory_reinforced",
    "graph_open_completed",
)

_AGENT_SUGGESTION_TOTAL = counter(
    "lethefield_agent_suggestion_total",
    "Agent 建议的人类处置结果计数（§19.4 留痕线；只计有 Agent 建议的记录）",
    ["outcome"],
    registry=REGISTRY,
)
_ESCALATION_TOTAL = counter(
    "lethefield_escalation_total",
    "升级事件计数（§19.4 留痕线，reason 对应 §11.2 四类）",
    ["reason"],
    registry=REGISTRY,
)
_FF_DELTA_APPLIED_TOTAL = counter(
    "lethefield_ff_delta_applied_total",
    "δ 调整触发计数（§19.2 标定线，从日志流聚合）",
    ["type"],
    registry=REGISTRY,
)
_GRAPH_OPEN_DURATION = histogram(
    "lethefield_graph_open_duration_seconds",
    "图打开耗时（显式 open 点客户端近似口径：provision/migrate/rebuild/schema 初始化）",
    ["type"],
    registry=REGISTRY,
)
_RECALLED_THEN_TOUCHED = gauge(
    "lethefield_ff_recalled_then_touched_rate",
    "召回节点 T 窗内被 reinforce 比例（λ 核心验收，离线聚合）",
    registry=REGISTRY,
)
_GRAPH_LRU_HIT_RATIO = gauge(
    "lethefield_graph_lru_cache_hit_ratio",
    "LRU 命中代理：1 −（闲置后首请求且 Stage 高耗时的请求占比）（离线推导口径）",
    registry=REGISTRY,
)
_CELL_WATERMARK = gauge(
    "lethefield_cell_watermark_ratio",
    "Cell 水位（映射表持久化值的暴露；设计名 cell_watermark，命名规则强制 _ratio）",
    ["cell_id", "dimension"],
    registry=REGISTRY,
)
_SPACE_STORAGE = gauge(
    "lethefield_space_storage_bytes",
    "单 space 存储字节按 tier 汇总（Cassandra size_estimates + ES 文档数比例分摊）",
    ["tier"],
    registry=REGISTRY,
)


@dataclass
class ExporterDeps:
    """依赖容器（测试逐项注入 fake）。"""

    es_ops: Any  # Elasticsearch（es-ops 日志集群）
    store: Any  # ControlPlaneStore（映射表：spaces/cells）
    cell_session: Any  # Cassandra cell 集群 session（system.size_estimates）
    ex_session: Any  # Cassandra EX 集群 session
    es_graph: Any  # Elasticsearch（rms_vectors _stats/_count 元数据）
    config: ExporterConfig = DEFAULT_CONFIG


def fetch_new_events(
    deps: ExporterDeps, search_after: list | None
) -> tuple[list[dict], list | None]:
    """按 (timestamp, _id) 游标推进读 es-ops 新增事件（单轮最多 1000 条，下轮续）。"""
    query: dict[str, Any] = {
        "size": 1000,
        "sort": [
            {"timestamp": "asc"}
        ],  # _id 不可排序（fielddata 限制）；同毫秒边界重读由 worker event_id 去重兜底
        "query": {"terms": {"event_type": list(TRACKED_EVENT_TYPES)}},
    }
    if search_after:
        query["search_after"] = search_after
    resp = deps.es_ops.search(index="lethefield-logs-*", body=query)
    hits = resp["hits"]["hits"]
    events = []
    for hit in hits:
        source = hit["_source"]
        source["_sort"] = hit["sort"]
        events.append(source)
    new_cursor = list(hits[-1]["sort"]) if hits else search_after
    return events, new_cursor


def fetch_events_window(
    es_ops, event_type: str, *, since_ms: int, until_ms: int
) -> list[dict[str, Any]]:
    """读某事件类型在 [since, until] 窗内的全部事件（gauge 重算口径，每轮全量重查）。"""
    resp = es_ops.search(
        index="lethefield-logs-*",
        body={
            "size": 10_000,
            "sort": [{"timestamp": "asc"}],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"event_type": event_type}},
                        {
                            "range": {
                                "timestamp": {
                                    "gte": since_ms,
                                    "lte": until_ms,
                                }
                            }
                        },
                    ]
                }
            },
        },
    )
    return [hit["_source"] for hit in resp["hits"]["hits"]]


def _keyspace_bytes(session) -> dict[str, int]:
    """system.size_estimates → per-keyspace 字节估算（mean_partition_size × partitions_count）。

    单节点 RF=1 开发期口径（多节点需按 RF 折算，届时标定）。
    """
    rows = session.execute(
        "SELECT keyspace_name, mean_partition_size, partitions_count FROM system.size_estimates"
    ).all()
    out: dict[str, int] = {}
    for row in rows:
        out[row.keyspace_name] = out.get(row.keyspace_name, 0) + int(
            row.mean_partition_size * row.partitions_count
        )
    return out


def _vector_doc_counts(es_graph, space_ids: list[str]) -> tuple[dict[str, int], int]:
    """rms_vectors：各 space 文档数（_count?routing=，O(1) 计数不扫内容）+ 索引总字节。"""
    counts: dict[str, int] = {}
    for space_id in space_ids:
        resp = es_graph.count(index="rms_vectors", routing=space_id)
        counts[space_id] = int(resp["count"])
    try:
        stats = es_graph.indices.stats(index="rms_vectors")
        total_bytes = int(stats["_all"]["total"]["store"]["size_in_bytes"])
    except Exception:
        total_bytes = 0  # 索引不存在（空栈）时按 0，不阻塞其他序列
    return counts, total_bytes


@redline1_exempt(
    worker="metrics-exporter",
    reason=(
        "只读 es-ops 运维日志流 + system.size_estimates 系统表 + 控制面映射表元数据 + "
        "rms_vectors _stats/_count（per-space routing O(1) 计数），不扫业务数据面；"
        "space/Cell 枚举走映射表 list_space_mappings/list_cells（元数据）"
    ),
    cadence="ExporterConfig.poll_interval_seconds 轮询",
)
def run_once(deps: ExporterDeps, search_after: list | None = None) -> list | None:
    """单轮：counter 增量折叠 + gauge 重算 + 元数据采集。返回新游标（进程内持有）。

    游标不持久化：进程重启从 es-ops 全量重折叠重建 counter（开发期日志量有界；
    es-ops 滚动清理后若重建成本成问题，届时给 counter 加状态持久化）。
    """
    events, new_cursor = fetch_new_events(deps, search_after)

    # 留痕线 / δ counter（增量折叠）
    folded = aggregations.fold_counters(events)
    for outcome, n in folded["agent_suggestion_total"].items():
        _AGENT_SUGGESTION_TOTAL.labels(outcome=outcome).inc(n)
    for reason, n in folded["escalation_total"].items():
        _ESCALATION_TOTAL.labels(reason=reason).inc(n)
    for delta_type, n in folded["ff_delta_applied_total"].items():
        _FF_DELTA_APPLIED_TOTAL.labels(type=delta_type).inc(n)
    # graph_open_duration histogram（显式 open 点，客户端近似口径）
    for e in events:
        if e.get("event_type") == "graph_open_completed":
            _GRAPH_OPEN_DURATION.labels(type=e["payload"]["open_type"]).observe(
                float(e["payload"]["duration_seconds"])
            )
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    w = deps.config.touched_window_ms

    # touched rate（窗口全量重算）
    recalls = fetch_events_window(
        deps.es_ops, "retrieve_recall_detail", since_ms=now_ms - w, until_ms=now_ms
    )
    reinforces = fetch_events_window(
        deps.es_ops, "memory_reinforced", since_ms=now_ms - w, until_ms=now_ms
    )
    rate = aggregations.recalled_then_touched_rate(recalls, reinforces, window_ms=w, now_ms=now_ms)
    if rate is not None:
        _RECALLED_THEN_TOUCHED.set(rate)

    # lru 命中代理（近窗全量重算）
    proxy = aggregations.lru_cache_hit_proxy(
        recalls,
        idle_gap_ms=deps.config.lru_idle_gap_ms,
        slow_stage_ms=deps.config.lru_slow_stage_ms,
    )
    if proxy is not None:
        _GRAPH_LRU_HIT_RATIO.set(proxy)

    # cell 水位（映射表持久化值暴露）
    for cell in deps.store.list_cells():
        for dimension, value in (cell.capacity or {}).items():
            _CELL_WATERMARK.labels(cell_id=cell.cell_id, dimension=dimension).set(float(value))

    # space 存储字节按 tier 汇总
    mappings = {m.space_id: m.tier.value for m in deps.store.list_space_mappings()}
    keyspace_bytes = _keyspace_bytes(deps.cell_session)
    for ks, nbytes in _keyspace_bytes(deps.ex_session).items():
        keyspace_bytes[ks] = keyspace_bytes.get(ks, 0) + nbytes
    counts, total_bytes = _vector_doc_counts(deps.es_graph, list(mappings))
    for tier, nbytes in aggregations.storage_bytes_by_tier(
        mappings, keyspace_bytes, counts, total_bytes
    ).items():
        _SPACE_STORAGE.labels(tier=tier).set(nbytes)
    return new_cursor
