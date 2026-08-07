"""Q2a 恢复脚本：清掉残留索引 -> 重建 mixed index -> 验证全文检索。

用法: .venv/bin/python scripts/q2a_recover.py
"""
import sys
import time

sys.path.insert(0, "scripts")
from common import submit  # noqa: E402


def main():
    # 1) 若残留 contentByText 且状态异常（INSTALLED），先 DROP
    r = submit(r"""
import org.janusgraph.core.schema.SchemaStatus
import org.janusgraph.core.schema.SchemaAction

mgmt = graph.openManagement()
idx = mgmt.getGraphIndex('contentByText')
out = []
if (idx != null) {
    st = idx.getIndexStatus(mgmt.getPropertyKey('content'))
    if (st != SchemaStatus.ENABLED) {
        mgmt.updateIndex(idx, SchemaAction.DROP_INDEX)
        mgmt.commit()
        out.add('dropped (was ' + st + ')')
    } else {
        mgmt.rollback()
        out.add('already ENABLED, keep')
    }
} else {
    mgmt.rollback()
    out.add('no residue')
}
out.toString()
""")
    print("cleanup:", r)
    time.sleep(3)

    # 2) 重建 mixed index（TEXT mapping，ES 后端）
    r = submit(r"""
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.schema.Mapping

mgmt = graph.openManagement()
created = false
if (mgmt.getGraphIndex('contentByText') == null) {
    if (mgmt.getPropertyKey('content') == null) {
        mgmt.makePropertyKey('content').dataType(String.class).make()
    }
    mgmt.buildIndex('contentByText', Vertex.class)
        .addKey(mgmt.getPropertyKey('content'), Mapping.TEXT.asParameter())
        .buildMixedIndex('search')
    created = true
}
mgmt.commit()
'created=' + created
""")
    print("build:", r)

    # 3) 等待 ENABLED
    r = submit(r"""
import org.janusgraph.core.schema.SchemaStatus
import org.janusgraph.graphdb.database.management.ManagementSystem
import java.time.temporal.ChronoUnit
ManagementSystem.awaitGraphIndexStatus(graph, 'contentByText')
    .status(SchemaStatus.ENABLED).timeout(120, ChronoUnit.SECONDS).call()
mgmt = graph.openManagement()
st = mgmt.getGraphIndex('contentByText').getIndexStatus(mgmt.getPropertyKey('content'))
mgmt.rollback()
'status=' + st
""")
    print("await:", r)

    # 4) 写入测试数据并全文检索
    r = submit(r"""
g = graph.traversal()
existing = g.V().has('name','dave').hasNext()
if (!existing) {
    dave = g.addV('person').property('name','dave').property('age',41).property('content','dave researches episodic memory decay curves in AI agents').next()
    erin = g.addV('person').property('name','erin').property('age',33).property('content','erin tunes vector indexes and knn recall benchmarks').next()
    g.addE('knows').from(dave).to(erin).property('since',2023).next()
    g.tx().commit()
}
'written (existing=' + existing + ')'
""")
    print("write:", r)
    time.sleep(3)

    hits = submit(r"""
import org.apache.tinkerpop.gremlin.process.traversal.TextP
g = graph.traversal()
g.V().has('content', TextP.containing('memory')).values('name').toList()
""")
    print("TextP.containing('memory') hits:", hits)
    names = [str(h) for h in hits]
    assert 'dave' in names, f"全文查询未命中 dave: {names}"
    print("Q2a PASS: mixed index 全文查询命中 dave")


if __name__ == "__main__":
    main()
