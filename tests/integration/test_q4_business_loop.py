"""q4：最小业务闭环（spike Q4 重建，四断言 CI 基线）。

链路形态与开发文档 M4 一致（简化版）：
ES kNN（带 space_id routing）→ 锚点 node_key → gremlin 时序 1 跳子图
→ 检索侧现算 s_effective → θ 硬过滤。

四断言：高分召回 / 低分过滤 / 衰减过滤 / 跨 space 隔离（零泄漏）。
"""

from conftest import index_vector, knn
from ff_utils import s_effective, theta_effective

GRAPH = "it_q4"
SPACE_A = "q4-space-a"
SPACE_B = "q4-space-b"
N_NOW = 100
QUERY_VECTOR = [1.0, 0.0, 0.0, 0.0]

# (node_key, s, n_last_touched, vector)
NODES_A = [
    ("a-hot", 0.9, 100, [1.0, 0.0, 0.0, 0.0]),  # 高分且新鲜 → 应召回
    ("a-decayed", 0.9, 80, [0.98, 0.2, 0.0, 0.0]),  # Δn=20 → 衰减至 ≈0.10 应过滤
    ("a-low", 0.05, 100, [0.96, 0.28, 0.0, 0.0]),  # 低分 → 应过滤
]
NODE_B = ("b-twin", 0.95, 100, [0.99, 0.14, 0.0, 0.0])  # 跨 space 相似内容 → 零泄漏


def _write_graph(gremlin) -> None:
    gremlin.ensure_graph(GRAPH)
    gremlin.clear_graph(GRAPH)

    def add_node(key, s, n_touched, space):
        return (
            f't.addV().property("node_key", "{key}").property("space_id", "{space}")'
            f'.property("content", "content of {key}")'
            f'.property("s", {s}d)'
            f'.property("n_created", {n_touched}L).property("n_last_touched", {n_touched}L).next()'
        )

    script = f"""
    def g = ConfiguredGraphFactory.open(gname)
    def t = g.traversal()
    def hot = {add_node("a-hot", 0.9, 100, SPACE_A)}
    def decayed = {add_node("a-decayed", 0.9, 80, SPACE_A)}
    def low = {add_node("a-low", 0.05, 100, SPACE_A)}
    def twin = {add_node("b-twin", 0.95, 100, SPACE_B)}
    hot.addEdge("temporal", decayed)
    decayed.addEdge("temporal", low)
    g.tx().commit()
    "written"
    """
    assert gremlin.submit(script, {"gname": GRAPH}) == ["written"]


def _write_vectors(es, vectors_index) -> None:
    for key, _, _, vector in NODES_A:
        index_vector(es, vectors_index, SPACE_A, key, vector)
    index_vector(es, vectors_index, SPACE_B, NODE_B[0], NODE_B[3])


def _subgraph(gremlin, space_id: str, anchor_keys: list[str]) -> dict:
    result = gremlin.submit(
        """
        def g = ConfiguredGraphFactory.open(gname)
        def t = g.traversal()
        def keyList = t.V().has("space_id", sid).has("node_key", P.within(anchorKeys))
                          .union(__.values("node_key"), __.both("temporal").values("node_key"))
                          .dedup().toList()
        def nodes = t.V().has("space_id", sid).has("node_key", P.within(keyList))
                       .valueMap("node_key", "s", "n_last_touched").toList()
        def edges = t.V().has("space_id", sid).has("node_key", P.within(keyList))
                       .bothE("temporal").dedup()
                       .project("out_key", "in_key")
                       .by(outV().values("node_key")).by(inV().values("node_key")).toList()
        ["nodes": nodes, "edges": edges]
        """,
        {"gname": GRAPH, "sid": space_id, "anchorKeys": anchor_keys},
    )
    # 服务端把返回 map 的每个 entry 作为独立结果项流回，先合并
    payload = {k: v for item in result for k, v in item.items()}
    nodes = {
        props["node_key"][0]: {"s": props["s"][0], "n_last_touched": props["n_last_touched"][0]}
        for props in payload["nodes"]
    }
    edges = {(edge["out_key"], edge["in_key"]) for edge in payload["edges"]}
    return {"nodes": nodes, "edges": edges}


def test_business_loop_four_assertions(gremlin, es, vectors_index):
    _write_graph(gremlin)
    _write_vectors(es, vectors_index)

    # --- Stage 2：ES kNN 锚点识别（带 space_id custom routing）---
    anchor_keys_a = {hit["node_key"] for hit in knn(es, vectors_index, SPACE_A, QUERY_VECTOR, k=5)}
    # 断言 4a：跨 space 隔离（ES 侧）——routing=A 的锚点集不含 B 的相似内容
    assert NODE_B[0] not in anchor_keys_a
    assert {"a-hot", "a-decayed", "a-low"} <= anchor_keys_a

    anchor_keys_b = {hit["node_key"] for hit in knn(es, vectors_index, SPACE_B, QUERY_VECTOR, k=5)}
    # 断言 4b：隔离对称成立——routing=B 只命中 B
    assert anchor_keys_b == {NODE_B[0]}

    # --- Stage 3（简化）：时序 1 跳子图，召回单元是带边子图而非孤立节点 ---
    subgraph = _subgraph(gremlin, SPACE_A, list(anchor_keys_a))
    nodes, edges = subgraph["nodes"], subgraph["edges"]

    # 断言 4c：跨 space 隔离（图侧）——子图中无 B 的节点
    assert NODE_B[0] not in nodes
    # 子图带边：时序链的两条边都在
    assert ("a-hot", "a-decayed") in edges
    assert ("a-decayed", "a-low") in edges

    # --- FF 现算 + θ 硬过滤 ---
    theta = theta_effective()
    kept = {
        key
        for key, props in nodes.items()
        if s_effective(props["s"], props["n_last_touched"], N_NOW) >= theta
    }

    # 断言 1：高分召回——s=0.9、Δn=0 的节点通过 θ 过滤
    assert "a-hot" in kept
    # 断言 2：低分过滤——s=0.05 被 θ 过滤
    assert "a-low" not in kept
    # 断言 3：衰减过滤——s=0.9 但 Δn=20 的节点现算 s_effective≈0.10 被过滤
    assert "a-decayed" not in kept
    decayed_eff = s_effective(0.9, 80, N_NOW)
    assert decayed_eff < theta, f"复刻 spike 场景：s_eff={decayed_eff:.3f} 应低于 θ={theta}"
    assert 0.05 < decayed_eff < 0.15, f"spike 参考值 s_eff≈0.10，实测 {decayed_eff:.3f}"

    # 最终召回结果恰为 {a-hot}
    assert kept == {"a-hot"}
