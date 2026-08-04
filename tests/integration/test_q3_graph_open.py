"""q3：graph open 延迟（spike Q3 重建）。

断言：ConfiguredGraphFactory 显式 createConfiguration 路径可用，
建图/冷开/热查询功能正常。延迟只记录不设硬阈值（spike 数字是容量基线，非 CI 门槛；
参考值：create p50≈0.9s、冷开 p50≈3.6s、热查询 p50≈48ms）。
"""

import time
import uuid

from conftest import GRAPH_BACKEND_PROPS


def test_dynamic_graph_create_open_and_query(gremlin):
    gname = f"it_q3_{uuid.uuid4().hex[:8]}"

    # create（建新图，keyspace + 表初始化）
    start = time.monotonic()
    gremlin.submit(
        """
        import org.janusgraph.core.ConfiguredGraphFactory
        import org.apache.commons.configuration2.MapConfiguration
        def props = backendProps + ["graph.graphname": gname, "storage.cql.keyspace": gname]
        ConfiguredGraphFactory.createConfiguration(new MapConfiguration(props))
        "created"
        """,
        {"gname": gname, "backendProps": GRAPH_BACKEND_PROPS},
    )
    create_seconds = time.monotonic() - start

    # open（冷开）
    start = time.monotonic()
    result = gremlin.submit(
        "ConfiguredGraphFactory.open(gname).traversal().V().count().next()",
        {"gname": gname},
    )
    open_seconds = time.monotonic() - start
    assert result == [0]

    # 热查询
    start = time.monotonic()
    gremlin.submit(
        "ConfiguredGraphFactory.open(gname).traversal().V().count().next()",
        {"gname": gname},
    )
    hot_query_seconds = time.monotonic() - start

    print(
        f"\n[q3] create={create_seconds:.2f}s open(cold)={open_seconds:.2f}s "
        f"hot_query={hot_query_seconds * 1000:.0f}ms"
    )

    # 清理：关闭图实例（保留 keyspace；不 DROP——红线 5 禁止在线 DROP keyspace，
    # 本地数据由 make reset 的 down -v 统一清理）
    gremlin.submit(
        "ConfiguredGraphFactory.close(gname); 'closed'",
        {"gname": gname},
    )
