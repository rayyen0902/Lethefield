"""CLI：python -m lethefield_ingest_dms [--once]

默认循环巡检（节奏由 LETHEFIELD_DMS_LOOP_INTERVAL_SECONDS 配置），--once 单轮
（测试/巡检用）。告警打印 LogEvent JSONL 到 stderr；本轮存在 page 级告警时
退出码 1，否则 0（对齐 clock_monitor 语义——loop 模式持续运行不退出）。

单路探测失败（admin REST 不可达、控制面连接失败等）不阻塞其他路，
失败本身即 page 级告警事件——DMS 不以"探测脚本没跑成"为摄入正常的证据。
"""

import argparse
import sys
import time
from datetime import UTC, datetime

from lethefield_clients import factories, redline1_exempt
from lethefield_clients.control_plane import MappingTableControlPlaneStore
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_event
from lethefield_metrics import metrics_port_from_env, start_metrics_server

from lethefield_ingest_dms.backlog import check_backlog, fetch_training_backlog, report_backlog
from lethefield_ingest_dms.config import DmsConfig
from lethefield_ingest_dms.freshness import check_freshness
from lethefield_ingest_dms.n_consistency import collect_n_consistency
from lethefield_ingest_dms.probe import (
    ensure_monitoring_topic,
    probe_pipeline,
    pulsar_probe_roundtrip,
)

SERVICE = "ingest-dms"

# ingest_dms /metrics 暴露口默认端口（M12 端口约定）
DEFAULT_METRICS_PORT = 9103


def _failure_event(event_type: str, error: Exception) -> LogEvent:
    """探测路径自身失败 → page 级告警（监控缺失不能静默）。"""
    return LogEvent(
        service=SERVICE,
        event_type=event_type,
        payload={"level": "page", "reason": "check_failed", "error": str(error)},
    )


@redline1_exempt(
    worker="ingest-dms",
    reason=(
        "space 枚举走 MappingTableControlPlaneStore.list_spaces()（freshness/n_consistency "
        "两路）；逐 space 独立比对（Redis 键 + EX MAX(n)，无跨 space 联合查询）；"
        "批间节流 = 轮询循环节奏"
    ),
    cadence="DmsConfig.loop_interval_seconds（env LETHEFIELD_DMS_LOOP_INTERVAL_SECONDS）",
)
def run_once(config: DmsConfig) -> list[LogEvent]:
    """单轮四路巡检，返回本轮全部告警事件（空 = 无告警）。"""
    alerts: list[LogEvent] = []
    now = datetime.now(UTC)
    redis = factories.redis_client()

    # 1. 管道活性探针（page 级）
    try:
        ensure_monitoring_topic(config.pulsar_admin_url)
        alerts.extend(probe_pipeline(lambda: pulsar_probe_roundtrip(config)))
    except Exception as exc:
        alerts.append(_failure_event("ingest_probe_failed", exc))

    # 2. 训练控制 backlog 监控（page 级；stats 查询失败 = 无法验证合规，按 page 告警）
    try:
        backlog = fetch_training_backlog(config.pulsar_admin_url)
        report_backlog(backlog)
        alerts.extend(
            check_backlog(redis, backlog, now=now, stale_seconds=config.backlog_stale_seconds)
        )
    except Exception as exc:
        alerts.append(_failure_event("training_control_backlog_stalled", exc))

    # 3. space 写入新鲜度（observation 级）
    try:
        store = MappingTableControlPlaneStore(factories.cassandra_cluster().connect())
        store.ensure_tables()
        alerts.extend(
            check_freshness(
                redis,
                store,
                now=now,
                activity_window_seconds=config.activity_window_seconds,
                stale_threshold_seconds=config.stale_threshold_seconds,
            )
        )
    except Exception as exc:
        alerts.append(_failure_event("space_write_stale", exc))

    # 4. n 一致性（page 级，M13 红线 3 配套）：Redis ex:n vs EX MAX(n)，
    #    n 回退 = 序号重复分配风险；连接建立与第 3 路同款先例（进程级巡检，随退出回收）
    try:
        n_store = MappingTableControlPlaneStore(factories.cassandra_cluster().connect())
        ex_session = factories.ex_cassandra_cluster().connect()
        alerts.extend(collect_n_consistency(redis, ex_session, n_store))
    except Exception as exc:
        alerts.append(_failure_event("ex_n_consistency_failed", exc))

    return alerts


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="ingest_dms", description="EX 摄入 Dead Man's Switch（M10）"
    )
    parser.add_argument("--once", action="store_true", help="单轮巡检（测试/巡检用）")
    args = parser.parse_args()
    config = DmsConfig.from_env()
    if not args.once:  # M12：常驻形态起 /metrics 暴露口（backlog/龄期 gauge 被 scrape）
        start_metrics_server(metrics_port_from_env(DEFAULT_METRICS_PORT))

    while True:
        alerts = run_once(config)
        for event in alerts:
            # 告警以结构化日志事件输出（告警通道选型属 M17 决策留痕项，此为事件源）
            emit_event(event, sync=args.once)
        has_page = any(e.payload.get("level") == "page" for e in alerts)
        if args.once:
            if not alerts:
                print("DMS 巡检通过：探针活性 / 训练控制 backlog / 写入新鲜度 / n 一致性均正常")
            return 1 if has_page else 0
        time.sleep(config.loop_interval_seconds)


if __name__ == "__main__":
    sys.exit(main())
