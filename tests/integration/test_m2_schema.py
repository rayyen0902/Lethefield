"""M2 RMS 图 Schema 验收的集成测试。

覆盖开发文档 §3 四条验收标准：
1. 顶点 schema 全量字段落地（16 属性键 + 类型 + 2 复合索引 + 5 边标签）
2. 四类边 + supersedes 边可正确建立；时序边 immutable（写入链之外无触碰路径）
3. 向量检索零泄漏：routing + space_id 过滤双机制，跨 space 查询 0 命中
4. ref_ex 抽样校验（RMS 侧不变量；EX 侧 join 待 M10）
"""

import json
import sys
import uuid
from datetime import UTC
from pathlib import Path

import pytest
from gremlin_python.driver.protocol import GremlinServerError

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import check_rms_schema  # noqa: E402
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL  # noqa: E402
from lethefield_clients import es_client, gremlin_client  # noqa: E402
from lethefield_rms.ff import n_star_horizon  # noqa: E402
from lethefield_rms.schema import ensure_graph_schema  # noqa: E402
from lethefield_rms.vectors import (  # noqa: E402  # noqa: E402
    VECTORS_INDEX,
    ensure_vectors_index,
    index_vector,
    knn_search,
)
from lethefield_rms.writer import create_edge, create_entity_node, create_event_node  # noqa: E402

SPACE = f"m2s-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def rms_graph():
    """唯一图名的全量 schema 图；清理只 close 图实例，不 DROP keyspace（红线 5）。"""
    gname = f"m2_{uuid.uuid4().hex[:8]}"
    client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(client, gname)
    yield client, gname
    client.submit("ConfiguredGraphFactory.close(gname); 'closed'", {"gname": gname}).all().result()
    client.close()


def _value_map(client, gname: str, node_key: str) -> dict:
    """读回顶点属性（valueMap 的 map 结果按 entry 逐个流回，先合并；值均为单元素列表）。"""
    result = (
        client.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal(); "
            "t.V().has('space_id', sid).has('node_key', nk).valueMap().next()",
            {"gname": gname, "sid": SPACE, "nk": node_key},
        )
        .all()
        .result()
    )
    merged = {k: v for item in result for k, v in item.items()}
    return {k: v[0] for k, v in merged.items()}


def test_full_schema_landed(rms_graph):
    client, gname = rms_graph
    assert check_rms_schema.static_check() == []
    assert check_rms_schema.inspect_graph(client, gname) == []


def test_event_node_roundtrip(rms_graph):
    client, gname = rms_graph
    base_tau = 1_720_000_000_000
    for i in range(3):
        create_event_node(
            client,
            gname,
            node_key=f"rt-{i}",
            space_id=SPACE,
            content=f"content of rt-{i}",
            tau_ms=base_tau + i * 1000,
            ref_ex=f"ex-rt-{i}",
            s=0.9 - i * 0.1,
            n_created=100 + i,
            agent_actor_id=f"actor-{i}",
            attrs={"tags": [f"t{i}"], "weight": i},
        )

    for i in range(3):
        props = _value_map(client, gname, f"rt-{i}")
        assert props["node_type"] == "event"
        assert props["content"] == f"content of rt-{i}"
        # tau 是 Date，读回为 naive datetime（UTC 语义），按 UTC 解释后毫秒时间戳必须相等
        assert round(props["tau"].replace(tzinfo=UTC).timestamp() * 1000) == base_tau + i * 1000
        assert props["s"] == pytest.approx(0.9 - i * 0.1)
        assert props["n_created"] == 100 + i
        # φ 初始化约定：n_last_touched = n_created，三计数器 0（M15 写入链约定）
        assert props["n_last_touched"] == 100 + i
        # n_star_cached 默认按 ff.n_star_horizon 自动计算（M4 起，写入链正确性兜底）
        assert props["n_star_cached"] == n_star_horizon(0.9 - i * 0.1, 100 + i, 0.3)
        assert props["reinforce_count"] == 0
        assert props["conflict_count"] == 0
        assert props["neglect_count"] == 0
        assert props["ref_ex"] == f"ex-rt-{i}"
        assert props["agent_actor_id"] == f"actor-{i}"
        assert json.loads(props["attrs"]) == {"tags": [f"t{i}"], "weight": i}


def test_node_key_unique(rms_graph):
    client, gname = rms_graph
    kwargs = {
        "node_key": "dup-1",
        "space_id": SPACE,
        "content": "dup",
        "tau_ms": 1_720_000_000_000,
        "ref_ex": "ex-dup-1",
        "s": 0.5,
        "n_created": 1,
    }
    create_event_node(client, gname, **kwargs)
    with pytest.raises(GremlinServerError):  # byNodeKey 唯一复合索引在 commit 时拒绝
        create_event_node(client, gname, **{**kwargs, "ref_ex": "ex-dup-1b"})


def test_five_edge_types(rms_graph):
    client, gname = rms_graph
    for i, key in enumerate(["n1", "n2", "n3"]):
        create_event_node(
            client,
            gname,
            node_key=key,
            space_id=SPACE,
            content=f"content of {key}",
            tau_ms=1_720_000_100_000 + i * 1000,
            ref_ex=f"ex-{key}",
            s=0.8,
            n_created=200 + i,
        )
    create_entity_node(client, gname, entity_key="e1", space_id=SPACE)

    create_edge(client, gname, space_id=SPACE, from_key="n1", to_key="n2", label="temporal")
    create_edge(client, gname, space_id=SPACE, from_key="n2", to_key="n3", label="temporal")
    create_edge(client, gname, space_id=SPACE, from_key="n1", to_key="n3", label="semantic")
    create_edge(client, gname, space_id=SPACE, from_key="n1", to_key="n3", label="causal")
    create_edge(client, gname, space_id=SPACE, from_key="n2", to_key="entity:e1", label="entity")
    create_edge(client, gname, space_id=SPACE, from_key="n3", to_key="n1", label="supersedes")

    result = (
        client.submit(
            """
        def t = ConfiguredGraphFactory.open(gname).traversal()
        def out = [:]
        out['n2_out_temporal'] = t.V().has('space_id', sid).has('node_key', 'n2')
            .out('temporal').values('node_key').toList()
        out['n2_in_temporal'] = t.V().has('space_id', sid).has('node_key', 'n2')
            .in('temporal').values('node_key').toList()
        out['n3_out_supersedes'] = t.V().has('space_id', sid).has('node_key', 'n3')
            .out('supersedes').values('node_key').toList()
        out['n2_out_entity'] = t.V().has('space_id', sid).has('node_key', 'n2')
            .out('entity').values('node_key').toList()
        out['e1_node_type'] = t.V().has('space_id', sid).has('node_key', 'entity:e1')
            .values('node_type').toList()
        out['n1_out_labels'] = t.V().has('space_id', sid).has('node_key', 'n1')
            .outE().label().toList()
        out
        """,
            {"gname": gname, "sid": SPACE},
        )
        .all()
        .result()
    )
    out = {k: v for item in result for k, v in item.items()}

    assert out["n2_out_temporal"] == ["n3"]
    assert out["n2_in_temporal"] == ["n1"]
    assert out["n3_out_supersedes"] == ["n1"]
    assert out["n2_out_entity"] == ["entity:e1"]
    assert out["e1_node_type"] == ["entity"]
    assert sorted(out["n1_out_labels"]) == ["causal", "semantic", "temporal"]


def test_vectors_zero_leak(rms_graph):
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)
    space_x, space_y = f"{SPACE}-x", f"{SPACE}-y"
    index_vector(es, space_id=space_x, node_key="x1", vector=[1.0, 0.0, 0.0, 0.0])
    index_vector(es, space_id=space_x, node_key="x2", vector=[0.9, 0.1, 0.0, 0.0])
    index_vector(es, space_id=space_y, node_key="y1", vector=[1.0, 0.0, 0.0, 0.0])
    index_vector(es, space_id=space_y, node_key="y2", vector=[0.0, 1.0, 0.0, 0.0])

    hits_x = knn_search(es, space_id=space_x, query_vector=[1.0, 0.0, 0.0, 0.0], k=5)
    assert {h["node_key"] for h in hits_x} == {"x1", "x2"}
    hits_y = knn_search(es, space_id=space_y, query_vector=[1.0, 0.0, 0.0, 0.0], k=5)
    assert {h["node_key"] for h in hits_y} == {"y1", "y2"}
    # 跨 space 零泄漏：查询无任何向量的 space，0 命中（routing + term 过滤双机制）
    hits_empty = knn_search(es, space_id=f"{SPACE}-empty", query_vector=[1.0, 0.0, 0.0, 0.0], k=5)
    assert hits_empty == []
    es.close()


def test_ref_ex_sampling(rms_graph):
    client, gname = rms_graph
    assert check_rms_schema.sample_ref_ex(client, gname) == []
