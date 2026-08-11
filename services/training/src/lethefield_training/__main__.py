"""CLI：python -m lethefield_training worker|scrub|submit-incident|ex-feed"""

import argparse
import os
import sys
import time
from pathlib import Path

from lethefield_clients import (
    AuthRegistryStore,
    FeedEvent,
    FeedKind,
    FeedSource,
    es_client,
    ex_cassandra_cluster,
    make_feed_publisher,
    pulsar_client,
)
from lethefield_logschema import configure as logschema_configure
from lethefield_metrics import metrics_port_from_env, start_metrics_server

from lethefield_training import ex_feed, recall_filter, worker
from lethefield_training.config import TrainingConfig
from lethefield_training.hot_store import HotSampleStore
from lethefield_training.recall_window import RecallWindow

# training worker /metrics 暴露口默认端口（M12 端口约定）
DEFAULT_METRICS_PORT = 9102

# es-ops 日志集群地址（shipper 同一 env，口径单点在 logschema es_sink）
_OPS_ES_URL = os.environ.get("LETHEFIELD_OPS_ES_URL", "http://localhost:9201")


def _worker_deps(config: TrainingConfig) -> worker.WorkerDeps:
    return worker.WorkerDeps(
        store=HotSampleStore(config.hot_root),
        window=RecallWindow(Path(config.hot_root) / "recall_window.jsonl", w_r3_ms=config.w_r3_ms),
        registry=AuthRegistryStore(),
        emit=worker.default_emit,
        config=config,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lethefield_training", description="训练数据管线（M11）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_worker = sub.add_parser("worker", help="加工 worker（feed + 契约 5 销毁处置）")
    p_worker.add_argument("--once", action="store_true", help="单轮后排空退出（测试/巡检用）")

    p_scrub = sub.add_parser("scrub", help="撤回授权的存量处置：内容字段清除、骨架保留（幂等）")
    p_scrub.add_argument("space_ref")

    p_incident = sub.add_parser("submit-incident", help="② 入料口：提交故障/混沌工程案例")
    for field in ("problem", "diagnosis", "decision", "outcome"):
        p_incident.add_argument(f"--{field}", required=True)
    p_incident.add_argument(
        "--space-ref", default=None, help="相关 space（可选；运维案例通常为无）"
    )

    p_ex = sub.add_parser("ex-feed", help="④ 入料口：EX 只读派生纠错对（显式单 space）")
    p_ex.add_argument("--space", required=True)

    p_filter = sub.add_parser(
        "recall-filter",
        help="③ 入料口过滤器（M12 定案形态）：es-ops 召回明细 → 授权闸门 → 训练 topic",
    )
    p_filter.add_argument("--once", action="store_true", help="单轮后退出（测试/巡检用）")
    p_filter.add_argument("--interval", type=float, default=5.0, help="轮询节奏（秒）")

    args = parser.parse_args(argv)
    config = TrainingConfig.from_env()

    if args.command == "worker":
        client = pulsar_client()
        try:
            deps = _worker_deps(config)
            if args.once:
                return 0 if worker.run_once(client, deps) >= 0 else 1
            # M12：常驻形态接日志管线 + /metrics 暴露口（一次性 --once 不起）
            logschema_configure()
            start_metrics_server(metrics_port_from_env(DEFAULT_METRICS_PORT))
            worker.run_forever(client, deps)
            return 0
        finally:
            client.close()
    if args.command == "scrub":
        count = HotSampleStore(config.hot_root).scrub(args.space_ref)
        print(f"scrubbed: {count} samples (space_ref={args.space_ref})")
        return 0
    if args.command == "submit-incident":
        client = pulsar_client()
        try:
            make_feed_publisher(client)(
                FeedEvent(
                    kind=FeedKind.INCIDENT,
                    source=FeedSource.INCIDENT,
                    space_ref=args.space_ref,
                    payload={
                        field: {"text": getattr(args, field)}
                        for field in ("problem", "diagnosis", "decision", "outcome")
                    },
                )
            )
        finally:
            client.close()
        print("incident submitted")
        return 0
    if args.command == "ex-feed":
        client = pulsar_client()
        session = ex_cassandra_cluster().connect()
        try:
            count = ex_feed.run(
                session,
                space_id=args.space,
                registry=AuthRegistryStore(),
                publish=make_feed_publisher(client),
                state_path=Path(config.hot_root) / "ex_feed_state" / f"{args.space}.json",
            )
        except PermissionError as e:
            print(str(e), file=sys.stderr)
            return 1
        finally:
            client.close()
        print(f"ex-feed: {count} correction pairs (space={args.space})")
        return 0
    if args.command == "recall-filter":
        es_ops = es_client(_OPS_ES_URL)
        client = pulsar_client()
        state_path = Path(config.hot_root) / "recall_filter_state.json"
        try:
            publish = make_feed_publisher(client)
            registry = AuthRegistryStore()
            while True:
                recall_filter.run_once(
                    es_ops,
                    registry=registry,
                    publish=publish,
                    state_path=state_path,
                )
                if args.once:
                    return 0
                time.sleep(args.interval)
        finally:
            client.close()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
