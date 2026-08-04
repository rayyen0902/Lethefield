"""q1：起栈 + 图读写（spike Q1 重建）。

断言：gremlin 就绪，顶点/边写入与 2 跳遍历正常。
"""

from conftest import wait_for_gremlin

GRAPH = "it_q1"


def test_graph_read_write_and_two_hop_traversal(gremlin):
    wait_for_gremlin()
    gremlin.ensure_graph(GRAPH)
    gremlin.clear_graph(GRAPH)

    # 写入 a→b→c 链
    result = gremlin.submit(
        """
        def g = ConfiguredGraphFactory.open(gname)
        def t = g.traversal()
        def a = t.addV().property("node_key", "a").property("space_id", "q1").next()
        def b = t.addV().property("node_key", "b").property("space_id", "q1").next()
        def c = t.addV().property("node_key", "c").property("space_id", "q1").next()
        a.addEdge("temporal", b)
        b.addEdge("temporal", c)
        g.tx().commit()
        "written"
        """,
        {"gname": GRAPH},
    )
    assert result == ["written"]

    # 1 跳：a 的直接邻居 = {b}
    one_hop = gremlin.submit(
        "ConfiguredGraphFactory.open(gname).traversal()"
        ".V().has('node_key', 'a').both('temporal').values('node_key').toList()",
        {"gname": GRAPH},
    )
    assert sorted(one_hop) == ["b"]

    # 2 跳：a 的两跳邻居 = {c}
    two_hop = gremlin.submit(
        "ConfiguredGraphFactory.open(gname).traversal()"
        ".V().has('node_key', 'a').repeat(both('temporal').simplePath()).times(2)"
        ".values('node_key').toList()",
        {"gname": GRAPH},
    )
    assert sorted(two_hop) == ["c"]
