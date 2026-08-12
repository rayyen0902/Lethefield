"""CLI：python -m lethefield_metrics_exporter [--once]"""

import argparse
import os
import time

from lethefield_clients import (
    MappingTableControlPlaneStore,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    redline1_exempt,
)
from lethefield_metrics import metrics_port_from_env, start_metrics_server

from lethefield_metrics_exporter.config import ExporterConfig
from lethefield_metrics_exporter.exporter import ExporterDeps, run_once

# es-ops 日志集群地址（shipper 同一 env，口径单点在 logschema es_sink）
_OPS_ES_URL = os.environ.get("LETHEFIELD_OPS_ES_URL", "http://localhost:9201")


@redline1_exempt(
    worker="metrics-exporter",
    reason=(
        "常驻聚合 worker 入口：聚合循环 exporter.run_once 已登记（三要件见其装饰器）；"
        "本入口不直接触存储，只组装依赖与节奏"
    ),
    cadence="ExporterConfig.poll_interval_seconds 轮询",
)
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="metrics_exporter",
        description="离线聚合指标 worker（M12）：日志管线/元数据 → Prometheus 指标",
    )
    parser.add_argument("--once", action="store_true", help="单轮聚合后退出（测试/巡检用）")
    args = parser.parse_args(argv)
    config = ExporterConfig.from_env()

    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    store = MappingTableControlPlaneStore(cell_cluster.connect())
    store.ensure_tables()
    deps = ExporterDeps(
        es_ops=es_client(_OPS_ES_URL),
        store=store,
        cell_session=cell_cluster.connect(),
        ex_session=ex_cluster.connect(),
        es_graph=es_client(),  # es-graph（rms_vectors 所在集群，默认 URL）
        config=config,
    )
    start_metrics_server(metrics_port_from_env(config.metrics_port))
    cursor = None
    try:
        while True:
            cursor = run_once(deps, cursor)
            if args.once:
                return 0
            time.sleep(config.poll_interval_seconds)
    finally:
        cell_cluster.shutdown()
        ex_cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
