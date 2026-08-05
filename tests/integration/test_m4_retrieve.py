"""M4 四阶段检索验收的集成测试（开发文档 §5 五条验收标准）。

场景（SPACE_X，n_now=100）：
- hot：s=0.9、Δn=0，高分新鲜 → 应召回
- decayed：s=0.9、Δn=20 → 粗筛/硬过滤排除（spike 衰减场景复刻）
- low：s=0.05 → 排除
- mid：s=0.5 → ρ=1 召回、ρ=0.5（θ=0.6）过滤（ρ 对照）
- sem/cau：经 semantic/causal 边连 hot；entity:e1 经 entity 边挂 hot
- supA 被 supB 取代（supB --supersedes--> supA）
- SPACE_Y 的 y1 与 hot 内容/向量几乎相同 → 跨 space 零泄漏
"""

import inspect
import uuid
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL
from lethefield_clients import es_client, gremlin_client
from lethefield_rms import ff
from lethefield_rms.ff import theta_effective
from lethefield_rms.retrieve import (
    DEFAULT_RETRIEVE_CONFIG,
    _stage2_anchors,
    _stage3_traverse,
    retrieve,
)
from lethefield_rms.schema import ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector
from lethefield_rms.writer import create_edge, create_entity_node, create_event_node

SPACE_X = f"m4x-{uuid.uuid4().hex[:8]}"
SPACE_Y = f"m4y-{uuid.uuid4().hex[:8]}"
N_NOW = 100
QUERY_VECTOR = [1.0, 0.0, 0.0, 0.0]
TAU = 1_720_000_000_000

# (node_key, s, n_created, vector, content)
EVENT_NODES = [
    ("hot", 0.9, 100, [1.0, 0.0, 0.0, 0.0], "project deadline meeting notes"),
    ("decayed", 0.9, 80, [0.98, 0.2, 0.0, 0.0], "project deadline old draft"),
    ("low", 0.05, 100, [0.96, 0.28, 0.0, 0.0], "project deadline trivial note"),
    ("mid", 0.5, 100, [0.9, 0.44, 0.0, 0.0], "project deadline budget followup"),
    ("sem", 0.9, 100, [0.8, 0.6, 0.0, 0.0], "semantic neighbor of the meeting"),
    ("cau", 0.9, 100, [0.7, 0.71, 0.0, 0.0], "causal consequence rollout"),
    ("supA", 0.9, 100, [0.99, 0.1, 0.0, 0.0], "legacy architecture decision"),
    ("supB", 0.9, 100, [0.85, 0.5, 0.0, 0.0], "current architecture decision"),
]


class CountingES:
    """ES 调用计数包装：验证 Stage 3 期间零 ES 调用（验收标准 2 的调用链追踪）。"""

    def __init__(self, real):
        self._real = real
        self.search_calls = 0

    def search(self, **kwargs):
        self.search_calls += 1
        return self._real.search(**kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture(scope="module")
def scenario():
    gname = f"m4_{uuid.uuid4().hex[:8]}"
    client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(client, gname)
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)

    for key, s, n_created, _v, content in EVENT_NODES:
        create_event_node(
            client,
            gname,
            node_key=key,
            space_id=SPACE_X,
            content=content,
            tau_ms=TAU,
            ref_ex=f"ex-{key}",
            s=s,
            n_created=n_created,
        )
    create_entity_node(client, gname, entity_key="e1", space_id=SPACE_X)
    create_edge(client, gname, space_id=SPACE_X, from_key="hot", to_key="mid", label="temporal")
    create_edge(client, gname, space_id=SPACE_X, from_key="hot", to_key="sem", label="semantic")
    create_edge(client, gname, space_id=SPACE_X, from_key="hot", to_key="cau", label="causal")
    create_edge(client, gname, space_id=SPACE_X, from_key="hot", to_key="entity:e1", label="entity")
    create_edge(client, gname, space_id=SPACE_X, from_key="supB", to_key="supA", label="supersedes")
    create_event_node(
        client,
        gname,
        node_key="y1",
        space_id=SPACE_Y,
        content="project deadline meeting notes",  # 与 hot 完全相同的文本
        tau_ms=TAU,
        ref_ex="ex-y1",
        s=0.95,
        n_created=100,
    )

    for key, _s, _n, vector, content in EVENT_NODES:
        index_vector(es, space_id=SPACE_X, node_key=key, vector=vector, content=content)
    index_vector(
        es,
        space_id=SPACE_Y,
        node_key="y1",
        vector=[0.99, 0.14, 0.0, 0.0],
        content="project deadline meeting notes",
    )

    yield SimpleNamespace(client=client, es=es, gname=gname)
    client.submit("ConfiguredGraphFactory.close(gname); 'closed'", {"gname": gname}).all().result()
    client.close()
    es.close()


def _retrieve(scenario, space_id=SPACE_X, **kwargs):
    kwargs.setdefault("query_vector", QUERY_VECTOR)
    kwargs.setdefault("n_now", N_NOW)
    return retrieve(scenario.client, scenario.es, scenario.gname, space_id=space_id, **kwargs)


def _keys(result):
    return {n.node_key for n in result.nodes}


def _edges(result):
    return {(e.out_key, e.in_key, e.label) for e in result.edges}


def test_returns_edged_subgraph(scenario):
    """验收 1：召回单元是带边子图（节点 + 时序/语义/因果/实体关系），非扁平列表。"""
    result = _retrieve(scenario)
    keys = _keys(result)
    assert {"hot", "mid", "sem", "cau", "supB", "entity:e1"} <= keys
    edges = _edges(result)
    assert ("hot", "mid", "temporal") in edges
    assert ("hot", "sem", "semantic") in edges
    assert ("hot", "cau", "causal") in edges
    assert ("hot", "entity:e1", "entity") in edges
    # 终态节点的 tau/content 随子图返回
    hot = next(n for n in result.nodes if n.node_key == "hot")
    assert hot.content == "project deadline meeting notes" or hot.brief


def test_stage3_makes_no_es_calls(scenario):
    """验收 2：Stage 3 期间无任何 ES 调用（签名隔离 + 调用计数双重验证）。"""
    params = inspect.signature(_stage3_traverse).parameters
    assert "es" not in params and "rho" not in params  # 物理隔离由签名强制

    counting = CountingES(scenario.es)
    anchors = _stage2_anchors(
        scenario.client,
        counting,
        scenario.gname,
        space_id=SPACE_X,
        query_text=None,
        query_vector=QUERY_VECTOR,
        n_now=N_NOW,
        theta=theta_effective(0.3, 1.0),
        config=DEFAULT_RETRIEVE_CONFIG,
        ff_config=ff.DEFAULT_CONFIG,
    )
    calls_after_stage2 = counting.search_calls
    assert calls_after_stage2 > 0  # Stage 2 确实走了 ES

    _stage3_traverse(
        scenario.client,
        scenario.gname,
        space_id=SPACE_X,
        anchors=anchors,
        n_now=N_NOW,
        trace_history=False,
        config=DEFAULT_RETRIEVE_CONFIG,
        ff_config=ff.DEFAULT_CONFIG,
    )
    assert counting.search_calls == calls_after_stage2  # Stage 3 期间零 ES 调用


def test_supersedes_redirect(scenario):
    """验收 3：默认返回取代者 supB、不返回 supA；显式追溯历史时返回 supA。"""
    default = _retrieve(scenario)
    assert "supB" in _keys(default)
    assert "supA" not in _keys(default)

    traced = _retrieve(scenario, trace_history=True)
    assert "supA" in _keys(traced)
    assert "supB" in _keys(traced)
    assert ("supB", "supA", "supersedes") in _edges(traced)


def test_rho_only_affects_hard_filters(scenario):
    """验收 4：固定 λ3 只变 ρ，Stage 3 遍历输出不变；ρ 只改变两处硬过滤。"""
    anchors = _stage2_anchors(
        scenario.client,
        scenario.es,
        scenario.gname,
        space_id=SPACE_X,
        query_text=None,
        query_vector=QUERY_VECTOR,
        n_now=N_NOW,
        theta=theta_effective(0.3, 1.0),
        config=DEFAULT_RETRIEVE_CONFIG,
        ff_config=ff.DEFAULT_CONFIG,
    )
    run1 = _stage3_traverse(
        scenario.client,
        scenario.gname,
        space_id=SPACE_X,
        anchors=anchors,
        n_now=N_NOW,
        trace_history=False,
        config=DEFAULT_RETRIEVE_CONFIG,
        ff_config=ff.DEFAULT_CONFIG,
    )
    run2 = _stage3_traverse(
        scenario.client,
        scenario.gname,
        space_id=SPACE_X,
        anchors=anchors,
        n_now=N_NOW,
        trace_history=False,
        config=DEFAULT_RETRIEVE_CONFIG,
        ff_config=ff.DEFAULT_CONFIG,
    )
    assert run1 == run2  # 遍历无 ρ 入参，结果确定（软惩罚排序与 ρ 无关）

    kept_rho1 = _keys(_retrieve(scenario, rho=1.0))
    kept_rho_strict = _keys(_retrieve(scenario, rho=0.5))  # θ 0.3 → 0.6
    assert "mid" in kept_rho1  # s_eff=0.5 过 θ=0.3
    assert "mid" not in kept_rho_strict  # 被 θ=0.6 过滤
    assert "hot" in kept_rho_strict  # s_eff=0.9 仍过
    assert kept_rho_strict < kept_rho1  # 差异只来自过滤，不是遍历路径


def test_cross_space_isolation(scenario):
    """验收 5：两 space 内容几乎相同，检索结果互不污染。"""
    result_x = _retrieve(scenario, space_id=SPACE_X)
    assert "y1" not in _keys(result_x)

    result_y = _retrieve(scenario, space_id=SPACE_Y)
    assert _keys(result_y) == {"y1"}


def test_end_to_end_q4_decay_scenario(scenario):
    """端到端复刻 q4：高分召回 / 低分过滤 / 衰减过滤（s=0.9、Δn=20）。"""
    keys = _keys(_retrieve(scenario))
    assert "hot" in keys  # 高分召回
    assert "low" not in keys  # 低分过滤
    assert "decayed" not in keys  # 衰减过滤（粗筛视界 90 < n_now=100）


def test_keyword_path(scenario):
    """Stage 2 关键词一路：query_text 走 ES match（content 字段），经 RRF 融合。"""
    result = _retrieve(scenario, query_vector=None, query_text="architecture decision")
    keys = _keys(result)
    assert "supB" in keys  # supA 命中后被重定向到 supB
    assert "y1" not in keys  # 关键词一路同样带 routing + term filter 双机制
