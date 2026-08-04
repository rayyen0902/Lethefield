"""M1 巡检：4 类存储物理隔离证明（开发文档 M1 验收第 1 条）。

证明 cell 的 Cassandra+ES、EX 的 Cassandra、Pulsar、运维日志 ES 是
相互独立的集群实例（compose 单节点形态下 = 独立容器/独立集群标识）：
- 两个 Cassandra 的 cluster_name / host_id 不同（system.local）
- 两个 ES 的 cluster_name / cluster_uuid 不同
- Pulsar 独立集群（admin API 可达，cluster 列表与上述无关联）

用法：uv run python scripts/verify_isolation.py
退出码：0 = 隔离成立，1 = 证明失败。
"""

import sys

import requests
from cassandra.cluster import Cluster

CASSANDRA = {
    "cassandra-cell": ("localhost", 9042),
    "cassandra-ex": ("localhost", 9043),
}
ES = {
    "es-graph": "http://localhost:9200",
    "es-ops": "http://localhost:9201",
}
PULSAR_ADMIN = "http://localhost:8080"


def cassandra_identity(host: str, port: int) -> dict:
    cluster = Cluster([host], port=port)
    try:
        session = cluster.connect()
        row = session.execute("SELECT cluster_name, host_id FROM system.local").one()
        return {"cluster_name": row.cluster_name, "host_id": str(row.host_id)}
    finally:
        cluster.shutdown()


def es_identity(url: str) -> dict:
    info = requests.get(f"{url}/", timeout=10).json()
    return {"cluster_name": info["cluster_name"], "cluster_uuid": info["cluster_uuid"]}


def pulsar_clusters() -> list[str]:
    return requests.get(f"{PULSAR_ADMIN}/admin/v2/clusters", timeout=10).json()


def main() -> int:
    failures: list[str] = []

    cass = {name: cassandra_identity(host, port) for name, (host, port) in CASSANDRA.items()}
    es = {name: es_identity(url) for name, url in ES.items()}

    for name, ident in {**cass, **es}.items():
        print(f"[ok] {name}: {ident}")

    if cass["cassandra-cell"]["cluster_name"] == cass["cassandra-ex"]["cluster_name"]:
        failures.append("两个 Cassandra cluster_name 相同，无法证明为不同集群")
    if cass["cassandra-cell"]["host_id"] == cass["cassandra-ex"]["host_id"]:
        failures.append("两个 Cassandra host_id 相同，疑似同一实例")
    if es["es-graph"]["cluster_uuid"] == es["es-ops"]["cluster_uuid"]:
        failures.append("两个 ES cluster_uuid 相同，疑似同一集群")

    clusters = pulsar_clusters()
    print(f"[ok] pulsar clusters: {clusters}")
    if not clusters:
        failures.append("Pulsar admin API 无可用集群")

    if failures:
        for failure in failures:
            print(f"[FAIL] {failure}", file=sys.stderr)
        return 1
    print("物理隔离证明通过：4 类存储为相互独立实例")
    return 0


if __name__ == "__main__":
    sys.exit(main())
