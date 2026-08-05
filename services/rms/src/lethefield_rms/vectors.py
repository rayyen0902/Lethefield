"""rms_vectors 独立向量索引（M2 定案形态）。

v_i 不进 JanusGraph、也不与全文/属性字段同索引共存（开发文档 §3 明确不做），
独立 ES 索引 rms_vectors，经 node_key 与图顶点关联，space_id 作 custom routing。
"""

from elasticsearch import Elasticsearch

VECTORS_INDEX = "rms_vectors"


def vectors_mapping(dims: int) -> dict:
    return {
        "properties": {
            "node_key": {"type": "keyword"},
            "space_id": {"type": "keyword"},
            # M4：Stage 2 关键词/属性检索由 ES 承担（设计文档 §5 v0.5），需要文本字段
            "content": {"type": "text"},
            "v": {
                "type": "dense_vector",
                "dims": dims,
                "index": True,
                "similarity": "cosine",
            },
        }
    }


def ensure_vectors_index(es: Elasticsearch, index: str = VECTORS_INDEX, dims: int = 4) -> None:
    """幂等建索引；已存在时校验 mapping 与期望一致，不符抛 ValueError（不静默放行）。

    已有索引缺 content 字段时 put_mapping 补齐（M2 旧索引 → M4 形态的幂等迁移）。
    """
    if not es.indices.exists(index=index):
        es.indices.create(index=index, mappings=vectors_mapping(dims))
        return
    properties = es.indices.get_mapping(index=index)[index]["mappings"]["properties"]
    for field in ("node_key", "space_id"):
        actual = properties.get(field, {}).get("type")
        if actual != "keyword":
            raise ValueError(f"索引 {index} 字段 {field} 类型为 {actual!r}，期望 'keyword'")
    v = properties.get("v", {})
    if v.get("type") != "dense_vector" or v.get("dims") != dims:
        raise ValueError(
            f"索引 {index} 的 v 为 {v.get('type')!r}/dims={v.get('dims')}，"
            f"期望 'dense_vector'/dims={dims}"
        )
    if "content" not in properties:
        es.indices.put_mapping(index=index, properties={"content": {"type": "text"}})


def index_vector(
    es: Elasticsearch,
    *,
    space_id: str,
    node_key: str,
    vector: list[float],
    content: str | None = None,
    index: str = VECTORS_INDEX,
    refresh: bool = True,
) -> None:
    """写入向量文档：routing = space_id（custom routing 定案），doc id 含 space 前缀便于清理。

    content 供 Stage 2 关键词检索（M4）；为 None 时不落该字段。
    """
    document = {"node_key": node_key, "space_id": space_id, "v": vector}
    if content is not None:
        document["content"] = content
    es.index(
        index=index,
        id=f"{space_id}:{node_key}",
        document=document,
        routing=space_id,
        refresh=refresh,
    )


def delete_vector(
    es: Elasticsearch,
    *,
    space_id: str,
    node_key: str,
    index: str = VECTORS_INDEX,
    refresh: bool = True,
) -> None:
    """删除向量文档（M6 归档时清理，防 Stage 2 召回死引用）；不存在则静默忽略。"""
    es.options(ignore_status=404).delete(
        index=index, id=f"{space_id}:{node_key}", routing=space_id, refresh=refresh
    )


def knn_search(
    es: Elasticsearch,
    *,
    space_id: str,
    query_vector: list[float],
    k: int,
    index: str = VECTORS_INDEX,
) -> list[dict]:
    """kNN 检索：space 隔离 = custom routing（分片级收拢）+ space_id 过滤（语义隔离）。

    红线：双机制缺一不可——routing 只保证同 space 文档聚到同一分片（检索精度/开销），
    不保证分片内无其他 space 的文档（分片是多 space 共享的），零泄漏语义必须由
    space_id term 过滤提供（M1 实测：只带 routing 会泄漏）。
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
    return [
        {"node_key": hit["_source"]["node_key"], "score": hit["_score"]}
        for hit in response["hits"]["hits"]
    ]


def keyword_search(
    es: Elasticsearch,
    *,
    space_id: str,
    query_text: str,
    k: int,
    index: str = VECTORS_INDEX,
) -> list[dict]:
    """关键词检索（M4 Stage 2 第二路）：match content + space_id term filter + routing。

    与 knn_search 同款双机制隔离：routing 收拢分片、term filter 保证跨 space 零泄漏。
    """
    response = es.search(
        index=index,
        query={
            "bool": {
                "must": {"match": {"content": query_text}},
                "filter": {"term": {"space_id": space_id}},
            }
        },
        routing=space_id,
        size=k,
    )
    return [
        {"node_key": hit["_source"]["node_key"], "score": hit["_score"]}
        for hit in response["hits"]["hits"]
    ]
