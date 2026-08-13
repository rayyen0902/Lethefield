"""CLI：python -m lethefield_writer worker [--once]（M15 写入链 worker）"""

import argparse

from lethefield_clients import (
    MappingTableControlPlaneStore,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    pulsar_client,
)
from lethefield_clients.redline import redline1_exempt
from lethefield_logschema import configure as logschema_configure
from lethefield_metrics import metrics_port_from_env, start_metrics_server
from lethefield_rms.quota import QuotaCounters
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index

from lethefield_writer import worker
from lethefield_writer.config import WriterConfig
from lethefield_writer.embedding import OpenAIEmbedder
from lethefield_writer.metrics import DEFAULT_METRICS_PORT


def _cmd_worker(args) -> int:
    config = WriterConfig.from_env()  # embedding 四变量缺失 fail-closed
    client = pulsar_client()
    ex_cluster = ex_cassandra_cluster()
    control_cluster = cassandra_cluster()
    gremlin = gremlin_client()
    es = es_client()
    try:
        # 共享 rms_vectors 索引由本服务（唯一写入方）接管：启动即建/校验 dims 与
        # mapping 一致，不符 fail-closed 拒启动（修订记录第 23 条④：索引内必须
        # 同模型同维度，模型变更 = 向量全量重建）
        ensure_vectors_index(es, index=VECTORS_INDEX, dims=config.embed_dims)
        store = MappingTableControlPlaneStore(control_cluster.connect())
        store.ensure_tables()
        deps = worker.WorkerDeps(
            gremlin=gremlin,
            es=es,
            ex_session=ex_cluster.connect(),
            embedder=OpenAIEmbedder(config),
            control_store=store,
            quota_counters=QuotaCounters(gremlin, es),
            emit=worker.default_emit,
            config=config,
        )
        if args.once:
            worker.run_once(client, deps)
            return 0
        # M12：常驻形态接日志管线 + /metrics 暴露口（一次性 --once 不起）
        logschema_configure()
        start_metrics_server(metrics_port_from_env(DEFAULT_METRICS_PORT))
        worker.run_forever(client, deps)
        return 0
    finally:
        client.close()
        gremlin.close()
        ex_cluster.shutdown()
        control_cluster.shutdown()


@redline1_exempt(
    worker="rms-writer",
    reason=(
        "消费 scoring-results Pulsar 消息（信封自带 space_id、与 topic 名校验），"
        "无枚举无跨 space 查询；建点写单 space 图（图名 = space_id），EX 反查/n "
        "连续性补偿按 space 单 keyspace 区间读取；故障隔离 = 逐消息处理"
    ),
    cadence="Pulsar 推送节奏；空轮 receive timeout（默认 1s）+ run_forever 轮间 sleep 1s",
)
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lethefield_writer", description="写入链 worker（M15：打分结果 → 图节点）"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_worker = sub.add_parser("worker", help="写入链 worker（scoring-results consumer，常驻）")
    p_worker.add_argument("--once", action="store_true", help="单轮后排空退出（测试/巡检用）")

    args = parser.parse_args(argv)
    if args.command == "worker":
        return _cmd_worker(args)
    return 2  # 不可达（subparsers required），防御


if __name__ == "__main__":
    raise SystemExit(main())
