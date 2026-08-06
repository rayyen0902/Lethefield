"""FS sweep worker 主循环（M6）。

每轮：list_spaces()（ControlPlaneStore 抽象——M9 起 MappingTableControlPlaneStore
按映射表 status=active 过滤，本文件调用接口零改动）→ 逐 space sweep_space →
写 Redis 心跳（全局 + 每 space）。停摆检测由 liveness 巡检承担（Dead Man's Switch，
sweep 停摆 = 忽视惩罚静默失效，设计文档 §7.5.1 同构故障）。

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
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    n_now,
    redis_client,
)
from lethefield_metrics import counter as _metric_counter
from lethefield_metrics import gauge as _metric_gauge
from lethefield_rms import ff
from prometheus_client import REGISTRY as _DEFAULT_REGISTRY
from redis import Redis

from lethefield_fs.config import DEFAULT_SWEEP_CONFIG, HEARTBEAT_KEY, SweepConfig
from lethefield_fs.sweep import SweepStats, sweep_space

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
    """执行一整轮 sweep（全部 space），写心跳与指标，返回每 space 计数。"""
    prev = redis.get(HEARTBEAT_KEY)
    if prev is not None:
        _SWEEP_LAG.set(time.time() - float(prev))

    results: dict[str, SweepStats] = {}
    for space in store.list_spaces():
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
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FS sweep worker（M6）")
    parser.add_argument("--once", action="store_true", help="执行一整轮后退出（测试/巡检用）")
    parser.add_argument("--interval", type=float, default=None, help="覆盖循环节奏（秒）")
    args = parser.parse_args(argv)

    config = DEFAULT_SWEEP_CONFIG
    if args.interval is not None:
        config = SweepConfig(
            sweep_interval_seconds=args.interval,
            consolidate_reinforce_threshold=config.consolidate_reinforce_threshold,
            near_horizon_margin=config.near_horizon_margin,
            stale_after_seconds=config.stale_after_seconds,
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
