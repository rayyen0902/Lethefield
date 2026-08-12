"""M4 四阶段检索单元测试：纯逻辑部分（图/ES 访问经注入替换）。

覆盖：RRF 融合（不掺 s）、转移分数软惩罚、supersedes 链解析（含链式/防环）、
束搜索（重定向/trace_history/束宽/深度/软惩罚不硬过滤）、Stage 4 双权重预算。
图侧端到端由 tests/integration/test_m4_retrieve.py 覆盖。
"""

import pytest
from lethefield_rms import ff
from lethefield_rms.retrieve import (
    DEFAULT_RETRIEVE_CONFIG,
    NodeProps,
    RetrieveConfig,
    ScoredNode,
    _beam_search,
    _NeighborRow,
    _resolve_chain,
    _rrf_merge,
    _stage4_budget,
    transition_score,
)

N_NOW = 100
CFG = DEFAULT_RETRIEVE_CONFIG
FF = ff.DEFAULT_CONFIG


def _props(key: str, s: float = 0.9, n: int = 100) -> NodeProps:
    return NodeProps(node_key=key, s=s, n_last_touched=n, content=f"content of {key}", tau=None)


def _anchor(key: str, rrf_score: float, s: float = 0.9, n: int = 100) -> ScoredNode:
    return ScoredNode(
        node_key=key,
        content=f"content of {key}",
        tau=None,
        s_effective=ff.s_effective(s, n, N_NOW),
        sim=rrf_score,
        path_score=rrf_score,
        depth=0,
    )


def _row(src: str, dst: NodeProps, label: str = "temporal", superseded_by=()) -> _NeighborRow:
    return _NeighborRow(
        src_key=src,
        out_key=src,
        in_key=dst.node_key,
        label=label,
        dst=dst,
        superseded_by=tuple(superseded_by),
    )


class TestRRF:
    def test_fusion_sums_reciprocal_ranks(self):
        rrf = _rrf_merge(
            [[{"node_key": "a"}, {"node_key": "b"}], [{"node_key": "b"}, {"node_key": "c"}]],
            rrf_k=60,
        )
        assert rrf["b"] == pytest.approx(1 / 62 + 1 / 61)  # 两路都命中：rank2 + rank1
        assert rrf["a"] == pytest.approx(1 / 61)
        assert rrf["c"] == pytest.approx(1 / 62)

    def test_inputs_are_ranks_only(self):
        # RRF 只消费 node_key 与名次——hit 里即使混入 s 也不影响结果（s 永不进 RRF）
        plain = _rrf_merge([[{"node_key": "a"}, {"node_key": "b"}]], rrf_k=60)
        polluted = _rrf_merge([[{"node_key": "a", "s": 0.01}, {"node_key": "b", "s": 0.99}]], 60)
        assert plain == polluted


class TestTransitionScore:
    def test_soft_penalty_no_hard_filter(self):
        # s_eff 再低也只是分数低，不会被硬过滤（桥节点保护）
        assert transition_score("temporal", 0.0, 0.001, CFG) > 0

    def test_monotonic_in_prior_and_s(self):
        causal = transition_score("causal", 0.0, 0.9, CFG)
        temporal = transition_score("temporal", 0.0, 0.9, CFG)
        assert causal > temporal  # edge_prior: causal 1.0 > temporal 0.6
        assert transition_score("temporal", 0.0, 0.9, CFG) > transition_score(
            "temporal", 0.0, 0.1, CFG
        )

    def test_s_zero_guarded(self):
        assert transition_score("temporal", 0.0, 0.0, CFG) > 0  # log(0) 兜底

    def test_no_rho_parameter(self):
        # ρ 物理隔离：转移分数签名里根本没有 ρ/θ（约束由签名强制）
        import inspect

        assert "rho" not in inspect.signature(transition_score).parameters
        assert "theta" not in inspect.signature(transition_score).parameters


class TestResolveChain:
    def test_chain_a_b_c(self):
        table = {"A": (_props("A"), ("B",)), "B": (_props("B"), ("C",)), "C": (_props("C"), ())}
        chain = _resolve_chain(lambda k: table.get(k), "A", max_chain=16)
        assert [p.node_key for p in chain] == ["A", "B", "C"]

    def test_cycle_guarded(self):
        table = {"A": (_props("A"), ("B",)), "B": (_props("B"), ("A",))}
        chain = _resolve_chain(lambda k: table.get(k), "A", max_chain=16)
        assert [p.node_key for p in chain] == ["A", "B"]  # 防环：回到 A 前停止

    def test_missing_node_breaks(self):
        chain = _resolve_chain(lambda k: None, "ghost", max_chain=16)
        assert chain == []


def _beam(anchors, expand_table, chains=None, trace_history=False, config=CFG):
    chains = chains or {}
    return _beam_search(
        anchors=anchors,
        n_now=N_NOW,
        trace_history=trace_history,
        config=config,
        ff_config=FF,
        expand_fn=lambda keys: [row for k in keys for row in expand_table.get(k, [])],
        resolve_fn=lambda k: chains.get(k, [_props(k)]),
    )


class TestBeamSearch:
    def test_expands_and_collects_edges(self):
        pool, edges = _beam(
            [_anchor("A", 0.5)],
            {"A": [_row("A", _props("B"), "causal"), _row("A", _props("C"))]},
        )
        assert set(pool) == {"A", "B", "C"}
        assert ("A", "B", "causal") in {(e.out_key, e.in_key, e.label) for e in edges}
        # 扩展节点 sim=0（方案 A）
        assert pool["B"].sim == 0.0
        assert pool["B"].depth == 1

    def test_soft_penalty_keeps_low_s_bridge(self):
        # s 极低的桥节点不被硬过滤，仍进候选池（由收敛后的 θ 硬过滤裁决）
        pool, _ = _beam([_anchor("A", 0.5)], {"A": [_row("A", _props("B", s=0.01))]})
        assert "B" in pool
        assert pool["B"].s_effective == pytest.approx(0.01)

    def test_supersedes_redirect_default(self):
        pool, edges = _beam(
            [_anchor("A", 0.5)],
            {"A": [_row("A", _props("B"), superseded_by=("D",))]},
            chains={"B": [_props("B"), _props("D")]},
        )
        assert "D" in pool and "B" not in pool  # 默认：取代者进池，被取代节点不进
        assert ("D", "B", "supersedes") in {(e.out_key, e.in_key, e.label) for e in edges}

    def test_supersedes_trace_history(self):
        pool, _ = _beam(
            [_anchor("A", 0.5)],
            {"A": [_row("A", _props("B"), superseded_by=("D",))]},
            chains={"B": [_props("B"), _props("D")]},
            trace_history=True,
        )
        assert "B" in pool and "D" in pool  # 显式追溯历史：被取代节点保留

    def test_superseded_anchor_redirected(self):
        pool, _ = _beam(
            [_anchor("A", 0.5)],
            {},
            chains={"A": [_props("A"), _props("B")]},
        )
        assert set(pool) == {"B"}  # 锚点本身被取代 → 重定向到 B

    def test_beam_width_limits_expansion(self):
        cfg = RetrieveConfig(beam_width=1)
        pool, _ = _beam(
            [_anchor("A", 0.5), _anchor("Z", 0.4)],
            {"A": [_row("A", _props("B"))], "Z": [_row("Z", _props("Y"))]},
            config=cfg,
        )
        assert "B" in pool and "Y" not in pool  # 束宽 1：只有最高分锚点被扩展

    def test_max_depth_bounds_traversal(self):
        pool, _ = _beam(
            [_anchor("A", 0.5)],
            {"A": [_row("A", _props("B"))], "B": [_row("B", _props("C"))]},
            config=RetrieveConfig(max_depth=1),
        )
        assert "B" in pool and "C" not in pool  # 深度 1：不再向下扩展

    def test_path_score_multiplies(self):
        pool, _ = _beam([_anchor("A", 0.5)], {"A": [_row("A", _props("B"), "causal")]})
        expected = 0.5 * transition_score("causal", 0.0, pool["B"].s_effective, CFG)
        assert pool["B"].path_score == pytest.approx(expected)


class TestStage4Budget:
    def _candidate(self, key, path_score, s_eff, content=None):
        return ScoredNode(
            node_key=key,
            content=content or f"content of {key}",
            tau=None,
            s_effective=s_eff,
            sim=0.0,
            path_score=path_score,
            depth=1,
        )

    def test_double_high_full_low_s_brief(self):
        cfg = RetrieveConfig(token_budget=10_000, brief_prefix_chars=5)
        hot = self._candidate("hot", path_score=1.0, s_eff=0.9, content="x" * 100)
        stale = self._candidate("stale", path_score=0.9, s_eff=0.2, content="y" * 100)
        items = {i.node_key: i for i in _stage4_budget([hot, stale], config=cfg)}
        assert not items["hot"].brief and items["hot"].content == "x" * 100  # 双高 → 完整
        assert items["stale"].brief and items["stale"].content == "y" * 5  # s 低 → 压缩

    def test_budget_greedy_skip_overflow(self):
        cfg = RetrieveConfig(token_budget=3, brief_prefix_chars=100)
        big = self._candidate("big", 1.0, 0.9, content="x" * 40)  # ≈10 token，超预算
        small = self._candidate("small", 0.9, 0.9, content="yy")  # 1 token，装得下
        keys = [i.node_key for i in _stage4_budget([big, small], config=cfg)]
        assert keys == ["small"]

    def test_entity_leaf_is_full_and_unscored(self):
        entity = ScoredNode("entity:e1", "e1", None, None, 0.0, 0.0, 2)
        (item,) = _stage4_budget([entity], config=RetrieveConfig())
        assert not item.brief and item.s_effective is None and item.relevance == 0.0

    def test_max_returned_nodes_truncates_in_order(self):
        cfg = RetrieveConfig(token_budget=10_000, max_returned_nodes=2)
        cands = [self._candidate(f"n{i}", path_score=1.0 - i * 0.01, s_eff=0.9) for i in range(5)]
        keys = [i.node_key for i in _stage4_budget(cands, config=cfg)]
        assert keys == ["n0", "n1"]  # 硬上限截断，保持原排序
