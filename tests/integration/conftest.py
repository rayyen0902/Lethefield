"""集成测试公共设施：Gremlin/ES 客户端、动态图创建、向量索引。

对应 spike q1–q4 的 CI 基线（开发文档 M0 任务 4）。四断言的语义锁定在
test_q4_business_loop.py；本文件提供可复用的图/索引基础设施。

M2 起 schema/向量索引实现归属 services/rms（lethefield_rms），此处只做薄封装，
保持既有 fixture 名称与函数签名不变。
"""

import time

import pytest
from elasticsearch import Elasticsearch
from lethefield_clients import gremlin_client
from lethefield_rms.schema import GRAPH_BACKEND_PROPS, ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, knn_search
from lethefield_rms.vectors import index_vector as _index_vector

GREMLIN_URL = "ws://localhost:8182/gremlin"
# 自定义 server yaml 只绑定 ConfigurationManagementGraph（动态图经 ConfiguredGraphFactory 管理），
# 客户端别名必须指向它，纯脚本提交用不到该别名，只需能解析。
GREMLIN_ALIAS = "ConfigurationManagementGraph"
ES_GRAPH_URL = "http://localhost:9200"
VECTOR_DIMS = 4

__all__ = [
    "GRAPH_BACKEND_PROPS",
    "VECTORS_INDEX",
    "index_vector",
    "knn",
    "wait_for_gremlin",
]


class Gremlin:
    def __init__(self) -> None:
        self._client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)

    def submit(self, script: str, bindings: dict | None = None) -> list:
        return self._client.submit(script, bindings or {}).all().result()

    def ensure_graph(self, gname: str) -> None:
        ensure_graph_schema(self._client, gname)

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
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=VECTOR_DIMS)
    return VECTORS_INDEX


def index_vector(es, index: str, space_id: str, node_key: str, vector: list[float]) -> None:
    """写入向量文档：routing = space_id（custom routing 定案），doc id 含 space 前缀便于清理。"""
    _index_vector(es, space_id=space_id, node_key=node_key, vector=vector, index=index)


def knn(es, index: str, space_id: str, query_vector: list[float], k: int = 5) -> list[dict]:
    """kNN 检索：space 隔离 = custom routing（分片级收拢）+ space_id 过滤（语义隔离）。

    对齐设计文档 §16.3 的"跨 space 隔离双重验证（ES routing + 图内 space_id）"：
    routing 只保证同 space 文档聚到同一分片（检索精度/开销），不保证分片内无其他
    space 的文档（分片是多 space 共享的），零泄漏语义必须由 space_id 过滤提供。
    """
    return knn_search(es, space_id=space_id, query_vector=query_vector, k=k, index=index)


def wait_for_gremlin(timeout: float = 120.0) -> None:
    """等待 gremlin server 可执行脚本（wait_for_stack.sh 之外的测试侧兜底）。"""
    deadline = time.time() + timeout
    while True:
        try:
            client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
            assert client.submit("1+1").all().result() == [2]
            client.close()
            return
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(2)
