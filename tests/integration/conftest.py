"""集成测试公共设施：Gremlin/ES 客户端、动态图创建、向量索引。

对应 spike q1–q4 的 CI 基线（开发文档 M0 任务 4）。四断言的语义锁定在
test_q4_business_loop.py；本文件提供可复用的图/索引基础设施。
"""

import time

import pytest
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client

GREMLIN_URL = "ws://localhost:8182/gremlin"
# 自定义 server yaml 只绑定 ConfigurationManagementGraph（动态图经 ConfiguredGraphFactory 管理），
# 客户端别名必须指向它，纯脚本提交用不到该别名，只需能解析。
GREMLIN_ALIAS = "ConfigurationManagementGraph"
ES_GRAPH_URL = "http://localhost:9200"
VECTORS_INDEX = "rms_vectors"
VECTOR_DIMS = 4

# 图后端指向 compose 内的 cell Cassandra + 图索引 ES（服务名，JG 容器内可达）
GRAPH_BACKEND_PROPS = {
    "storage.backend": "cql",
    "storage.hostname": "cassandra-cell",
    "index.search.backend": "elasticsearch",
    "index.search.hostname": "es-graph",
    "cache.db-cache": "false",
}

# 动态建图 + schema 初始化（幂等）。schema 对齐开发文档 M2 节点字段的最小子集：
# node_key / space_id / content / s / n_created / n_last_touched + temporal 边。
ENSURE_GRAPH_SCRIPT = """
import org.janusgraph.core.ConfiguredGraphFactory
import org.apache.commons.configuration2.MapConfiguration
import org.apache.tinkerpop.gremlin.structure.Vertex

def g
try {
    g = ConfiguredGraphFactory.open(gname)
} catch (Exception ignored) {
    def props = backendProps + ["graph.graphname": gname, "storage.cql.keyspace": gname]
    ConfiguredGraphFactory.createConfiguration(new MapConfiguration(props))
    g = ConfiguredGraphFactory.open(gname)
}
def mgmt = g.openManagement()
if (mgmt.getPropertyKey("node_key") == null) {
    def nodeKey = mgmt.makePropertyKey("node_key").dataType(String.class).make()
    mgmt.makePropertyKey("space_id").dataType(String.class).make()
    mgmt.makePropertyKey("content").dataType(String.class).make()
    mgmt.makePropertyKey("s").dataType(Double.class).make()
    mgmt.makePropertyKey("n_created").dataType(Long.class).make()
    mgmt.makePropertyKey("n_last_touched").dataType(Long.class).make()
    mgmt.makeEdgeLabel("temporal").make()
    mgmt.buildIndex("byNodeKey", Vertex.class).addKey(nodeKey).unique().buildCompositeIndex()
    mgmt.commit()
} else {
    mgmt.rollback()
}
"ok"
"""


class Gremlin:
    def __init__(self) -> None:
        self._client = Client(GREMLIN_URL, GREMLIN_ALIAS)

    def submit(self, script: str, bindings: dict | None = None) -> list:
        return self._client.submit(script, bindings or {}).all().result()

    def ensure_graph(self, gname: str) -> None:
        self.submit(
            ENSURE_GRAPH_SCRIPT,
            {"gname": gname, "backendProps": GRAPH_BACKEND_PROPS},
        )

    def clear_graph(self, gname: str) -> None:
        self.submit(
            "def g = ConfiguredGraphFactory.open(gname); "
            "g.traversal().V().drop().iterate(); g.tx().commit(); 'ok'",
            {"gname": gname},
        )

    def close(self) -> None:
        self._client.close()


@pytest.fixture(scope="session")
def gremlin():
    client = Gremlin()
    yield client
    client.close()


@pytest.fixture(scope="session")
def es():
    client = Elasticsearch(ES_GRAPH_URL)
    yield client
    client.close()


@pytest.fixture(scope="session")
def vectors_index(es):
    """rms_vectors 独立向量索引（M2 定案形态）：dense_vector + node_key 关联，space_id routing。"""
    if not es.indices.exists(index=VECTORS_INDEX):
        es.indices.create(
            index=VECTORS_INDEX,
            mappings={
                "properties": {
                    "node_key": {"type": "keyword"},
                    "space_id": {"type": "keyword"},
                    "v": {
                        "type": "dense_vector",
                        "dims": VECTOR_DIMS,
                        "index": True,
                        "similarity": "cosine",
                    },
                }
            },
        )
    return VECTORS_INDEX


def index_vector(es, index: str, space_id: str, node_key: str, vector: list[float]) -> None:
    """写入向量文档：routing = space_id（custom routing 定案），doc id 含 space 前缀便于清理。"""
    es.index(
        index=index,
        id=f"{space_id}:{node_key}",
        document={"node_key": node_key, "space_id": space_id, "v": vector},
        routing=space_id,
        refresh=True,
    )


def knn(es, index: str, space_id: str, query_vector: list[float], k: int = 5) -> list[dict]:
    """kNN 检索：space 隔离 = custom routing（分片级收拢）+ space_id 过滤（语义隔离）。

    对齐设计文档 §16.3 的"跨 space 隔离双重验证（ES routing + 图内 space_id）"：
    routing 只保证同 space 文档聚到同一分片（检索精度/开销），不保证分片内无其他
    space 的文档（分片是多 space 共享的），零泄漏语义必须由 space_id 过滤提供。
    """
    response = es.search(
        index=index,
        knn={
            "field": "v",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": max(k * 4, 20),
            "filter": {"term": {"space_id": space_id}},
        },
        routing=space_id,
        size=k,
    )
    return [hit["_source"] for hit in response["hits"]["hits"]]


def wait_for_gremlin(timeout: float = 120.0) -> None:
    """等待 gremlin server 可执行脚本（wait_for_stack.sh 之外的测试侧兜底）。"""
    deadline = time.time() + timeout
    while True:
        try:
            client = Client(GREMLIN_URL, GREMLIN_ALIAS)
            assert client.submit("1+1").all().result() == [2]
            client.close()
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)
