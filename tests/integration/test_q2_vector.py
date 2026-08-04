"""q2：dense_vector 与图索引协同（spike Q2 重建）。

断言：rms_vectors 独立向量索引写入与 kNN 检索正常，
node_key 可与图顶点一一关联；routing 隔离生效。
"""

from conftest import index_vector, knn

SPACE = "q2-space"


def test_vector_index_and_knn(es, vectors_index):
    index_vector(es, vectors_index, SPACE, "k1", [1.0, 0.0, 0.0, 0.0])
    index_vector(es, vectors_index, SPACE, "k2", [0.0, 1.0, 0.0, 0.0])
    index_vector(es, vectors_index, SPACE, "k3", [0.0, 0.0, 1.0, 0.0])

    hits = knn(es, vectors_index, SPACE, [0.9, 0.1, 0.0, 0.0], k=2)
    assert hits[0]["node_key"] == "k1"
    assert len(hits) == 2


def test_routing_isolates_other_space(es, vectors_index):
    # 同一查询换一个 space（routing + space_id 过滤双重机制），应零命中
    hits = knn(es, vectors_index, "q2-other-space", [0.9, 0.1, 0.0, 0.0], k=2)
    assert hits == []
