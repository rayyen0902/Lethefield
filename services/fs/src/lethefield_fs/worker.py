"""FS sweep worker 主循环（M6）。

每轮：list_spaces()（ControlPlaneStore 抽象——M9 起 MappingTableControlPlaneStore
按映射表 status=active 过滤，本文件调用接口零改动）→ 逐 space sweep_space →
写 Redis 心跳（全局 + 每 space）。停摆检测由 liveness 巡检承担（Dead Man's Switch，
sweep 停摆 = 忽视惩罚静默失效，设计文档 §7.5.1 同构故障）。

M13 红线 3 冷热分频：store 为映射表实现时按 tier 分频——cold space 按
SweepConfig.cold_interval_seconds 降频（per-space 心跳键复用为 last-swept 时间戳），
hot/premium/未知 tier 按热节奏（保障优先）。冷 space 未到期即跳过是预期行为：
不写心跳、不计指标。注意 liveness 语义不变——全局键仍每轮写、
stale_after_seconds 仍管全局停摆检测；冷 space 的 per-space 心跳间隔变长
是设计预期，不是停摆信号（liveness 只读全局键，不受分频影响）。

图名 = space_id（M5 冻结契约；sweep 枚举源即映射表 active 集合）。
"""

import argparse
import time

from cassandra.cluster import Session as CassandraSession
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients import (
    ControlPlaneStore,
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    n_now,
    redis_client,
    redline1_exempt,
)
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_event
from lethefield_metrics import counter as _metric_counter
from lethefield_metrics import gauge as _metric_gauge
from lethefield_metrics import metrics_port_from_env, start_metrics_server
from lethefield_rms import ff
from prometheus_client import REGISTRY as _DEFAULT_REGISTRY
from redis import Redis

from lethefield_fs.config import DEFAULT_SWEEP_CONFIG, HEARTBEAT_KEY, SweepConfig, sweep_due
from lethefield_fs.sweep import SweepStats, sweep_space

# fs sweep 进程 /metrics 暴露口默认端口（M12 端口约定，env LETHEFIELD_METRICS_PORT 可覆盖）
DEFAULT_METRICS_PORT = 9101

# §19.3 告警线指标；注册进 prometheus 默认 registry（服务暴露口 M12 统一接线）
_SWEEP_PROCESSED = _metric_counter(
    "lethefield_fs_sweep_processed_total",
    "FS sweep 处理计数（按结果分类）",
    labels=["result"],
    registry=_DEFAULT_REGISTRY,
)
_SWEEP_LAG = _metric_gauge(
    "lethefield_fs_sweep_lag_seconds",
    "距上一轮成功 sweep 的时延（Dead Man's Switch 旁证）",
    registry=_DEFAULT_REGISTRY,
)


def _read_last_swept(redis: Redis, space: str) -> float | None:
    """读 per-space 心跳作 last-swept 时间戳；缺失或解析失败按 None（恒 due，fail-open 向扫）。"""
    raw = redis.get(f"{HEARTBEAT_KEY}:{space}")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


@redline1_exempt(
    worker="fs-sweep",
    reason=(
        "枚举走 ControlPlaneStore.list_spaces()（映射表 active 集合）；"
        "逐 space 独立 sweep_space（单 space 图 + per-space n_now，无跨 space 联合查询）；"
        "批间节流 = 轮间 sleep + 冷 space 分频跳过"
    ),
    cadence="SweepConfig.sweep_interval_seconds（默认 60s）；cold 按 cold_interval_seconds 降频",
)
def run_once(
    store: ControlPlaneStore,
    client: Client,
    ex_session: CassandraSession,
    cell_session: CassandraSession,
    es: Elasticsearch,
    redis: Redis,
    *,
    config: SweepConfig = DEFAULT_SWEEP_CONFIG,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> dict[str, SweepStats]:
    """执行一整轮 sweep，写心跳与指标，返回每 space 计数（本轮跳过的 space 不进返回值）。

    红线 3（M13）：store 为映射表实现时每轮重取 tier 映射分频，cold space 未到期
    跳过（不写心跳、不计指标）；取不到 tier 信息的 space 一律按 hot 保障优先。
    """
    prev = redis.get(HEARTBEAT_KEY)
    if prev is not None:
        _SWEEP_LAG.set(time.time() - float(prev))

    # tier 映射每轮重取（控制面规模，开销可忽略）；非映射表实现取不到 tier，全按 hot
    tiers: dict[str, Tier] = {}
    if isinstance(store, MappingTableControlPlaneStore):
        tiers = {m.space_id: m.tier for m in store.list_space_mappings()}

    results: dict[str, SweepStats] = {}
    for space in store.list_spaces():
        if not sweep_due(tiers.get(space), _read_last_swept(redis, space), time.time(), config):
            continue  # 冷 space 未到期：本轮跳过（红线 3 预期行为）
        stats = sweep_space(
            client,
            cell_session,
            es,
            gname=space,
            space_id=space,
            n_now=n_now(redis, ex_session, space_id=space),
            config=config,
            ff_config=ff_config,
        )
        for result_name in ("neglected", "archived", "consolidated", "refreshed"):
            _SWEEP_PROCESSED.labels(result=result_name).inc(getattr(stats, result_name))
        redis.set(f"{HEARTBEAT_KEY}:{space}", time.time())
        results[space] = stats
    redis.set(HEARTBEAT_KEY, time.time())
    # M12：δ 明细事件（ff_delta_applied_total 离线聚合数据源；count=0 不发控噪声）
    total_neglected = sum(s.neglected for s in results.values())
    if total_neglected:
        emit_event(
            LogEvent(
                service="lethefield-fs",
                event_type="ff_delta_applied",
                payload={"type": "neglect", "count": total_neglected},
            )
        )
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FS sweep worker（M6）")
    parser.add_argument("--once", action="store_true", help="执行一整轮后退出（测试/巡检用）")
    parser.add_argument("--interval", type=float, default=None, help="覆盖循环节奏（秒）")
    args = parser.parse_args(argv)

    config = DEFAULT_SWEEP_CONFIG
    if args.interval is not None:
        # 只覆盖热节奏（sweep_interval_seconds）；冷节奏 cold_interval_seconds 不受影响
        config = SweepConfig(
            sweep_interval_seconds=args.interval,
            consolidate_reinforce_threshold=config.consolidate_reinforce_threshold,
            near_horizon_margin=config.near_horizon_margin,
            stale_after_seconds=config.stale_after_seconds,
            cold_interval_seconds=config.cold_interval_seconds,
        )

    ex_cluster = ex_cassandra_cluster()
    cell_cluster = cassandra_cluster()
    ex_session = ex_cluster.connect()
    cell_session = cell_cluster.connect()
    client = gremlin_client()
    es = es_client()
    redis = redis_client()
    store = MappingTableControlPlaneStore(cell_session)
    store.ensure_tables()
    if not args.once:  # M12：常驻形态起 /metrics 暴露口（--once 短命进程不起）
        start_metrics_server(metrics_port_from_env(DEFAULT_METRICS_PORT))
    try:
        while True:
            results = run_once(store, client, ex_session, cell_session, es, redis, config=config)
            total = {
                name: sum(getattr(s, name) for s in results.values())
                for name in ("neglected", "archived", "consolidated", "refreshed")
            }
            print(f"[sweep] spaces={len(results)} {total}")
            if args.once:
                return 0
            time.sleep(config.sweep_interval_seconds)
    finally:
        client.close()
        ex_cluster.shutdown()
        cell_cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
