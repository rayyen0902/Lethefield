"""Q2: dense_vector kNN 与 JanusGraph mixed index 的协同验证。

a) JG mixed index (ES 后端) 全文查询
b) 尝试在 JG 的 ES 索引上加 dense_vector 并 kNN
c) 独立向量索引 + custom routing 的 kNN
d) 输出实现路径结论

用法: .venv/bin/python scripts/q2_vector_index.py
"""
import json
import sys
import time

import requests

sys.path.insert(0, "scripts")
from common import DIMS, ES_URL, cluster_centers, cosine, es, submit, synth_vector, make_rng  # noqa: E402

JG_INDEX = "janusgraph_contentbytext"   # JG 为每个 mixed index 在 ES 建独立索引（实测名）
VEC_INDEX = "rms_vectors"


def hr(t):
    print(f"\n===== {t} =====")


def part_a():
    hr("Q2a: JanusGraph mixed index (ES 后端) 全文检索")
    r = submit(r"""
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.schema.Mapping
import org.janusgraph.core.schema.SchemaStatus
import org.janusgraph.graphdb.database.management.ManagementSystem
import java.time.temporal.ChronoUnit

mgmt = graph.openManagement()
if (mgmt.getPropertyKey('content') == null) {
    mgmt.makePropertyKey('content').dataType(String.class).make()
}
created = false
if (mgmt.getGraphIndex('contentByText') == null) {
    contentKey = mgmt.getPropertyKey('content')
    mgmt.buildIndex('contentByText', Vertex.class)
        .addKey(contentKey, Mapping.TEXT.asParameter())
        .buildMixedIndex('search')
    created = true
}
mgmt.commit()
if (created) {
    ManagementSystem
        .awaitGraphIndexStatus(graph, 'contentByText')
        .status(SchemaStatus.ENABLED)
        .timeout(120, ChronoUnit.SECONDS).call()
}
mgmt2 = graph.openManagement()
status = mgmt2.getGraphIndex('contentByText').getIndexStatus(mgmt2.getPropertyKey('content'))
mgmt2.rollback()
'index contentByText status=' + status
""")
    print("management:", r)

    submit(r"""
g = graph.traversal()
dave = g.addV('person').property('name','dave').property('age',41).property('content','dave researches episodic memory decay curves in AI agents').next()
erin = g.addV('person').property('name','erin').property('age',33).property('content','erin tunes vector indexes and knn recall benchmarks').next()
g.addE('knows').from(dave).to(erin).property('since',2023).next()
g.tx().commit()
'ok'
""")
    time.sleep(2)  # 等 ES refresh
    hits = submit(r"""
import org.apache.tinkerpop.gremlin.process.traversal.TextP
g = graph.traversal()
g.V().has('content', TextP.containing('memory')).values('name').toList()
""")
    print("textContains('memory') hits:", hits)
    return hits


def show_jg_index_meta():
    r = requests.get(ES_URL + "/_cat/indices?format=json", timeout=30)
    print("ES indices:", [(i["index"], i.get("docs.count"), i.get("pri")) for i in r.json()])
    m = requests.get(ES_URL + f"/{JG_INDEX}/_mapping", timeout=30)
    if m.status_code == 200:
        mm = m.json()[JG_INDEX]["mappings"]
        print(f"{JG_INDEX} mapping: dynamic={mm.get('dynamic')}, fields={sorted(mm.get('properties', {}).keys())}")
        print(f"{JG_INDEX} _meta:", mm.get("_meta"))
    else:
        print(f"{JG_INDEX} mapping GET -> {m.status_code}: {m.text[:300]}")
    s = requests.get(ES_URL + f"/{JG_INDEX}/_settings", timeout=30)
    if s.status_code == 200:
        st = s.json()[JG_INDEX]["settings"]["index"]
        print(f"{JG_INDEX} settings: shards={st.get('number_of_shards')}, replicas={st.get('number_of_replicas')}")


def knn_query(index, vec, k=3, num_candidates=20, routing=None, filt=None):
    body = {"knn": {"field": "embedding", "query_vector": [float(x) for x in vec],
                    "k": k, "num_candidates": num_candidates}, "size": k}
    if routing:
        body["knn"]["filter"] = filt or {"term": {"space_id": routing}}
        params = {"routing": routing}
    else:
        params = {}
    r = requests.post(ES_URL + f"/{index}/_search", params=params, json=body, timeout=30)
    return r


def part_b(centers):
    hr("Q2b: 在 JG 的 ES 索引上加 dense_vector（同索引方案可行性）")
    show_jg_index_meta()
    # 1) 改 mapping: 加 embedding (dense_vector) + space_id (keyword)
    put = requests.put(ES_URL + f"/{JG_INDEX}/_mapping", json={
        "properties": {
            "embedding": {"type": "dense_vector", "dims": DIMS, "index": True, "similarity": "cosine"},
            "space_id": {"type": "keyword"},
        }}, timeout=30)
    print(f"PUT _mapping -> {put.status_code}: {put.text[:400]}")
    if put.status_code != 200:
        return False
    # 2) 直接写向量文档（绕过 JG，同索引）
    rng = make_rng("q2b")
    docs = []
    for ci in range(3):
        for si, space in enumerate(("spaceA", "spaceB")):
            v = synth_vector(centers[ci], 0.05, rng)
            doc = {"embedding": v.tolist(), "space_id": space, "label": f"c{ci}-{space}"}
            es("POST", f"/{JG_INDEX}/_doc?refresh=true", json=doc)
            docs.append((ci, space))
    # 3) kNN 查询：查 center0 最近邻
    q = synth_vector(centers[0], 0.02, make_rng("q2b-q"))
    r = knn_query(JG_INDEX, q, k=4)
    print(f"kNN on {JG_INDEX} -> {r.status_code}")
    if r.status_code == 200:
        hits = [(h["_source"].get("label"), round(h["_score"], 4)) for h in r.json()["hits"]["hits"]]
        print("kNN hits:", hits)
        return True
    print("kNN body:", r.text[:500])
    return False


def part_c(centers):
    hr("Q2c: 独立向量索引 rms_vectors + custom routing")
    requests.delete(ES_URL + f"/{VEC_INDEX}", timeout=30)
    es("PUT", f"/{VEC_INDEX}", json={
        "settings": {"number_of_shards": 2, "number_of_replicas": 0},
        "mappings": {"properties": {
            "embedding": {"type": "dense_vector", "dims": DIMS, "index": True, "similarity": "cosine"},
            "space_id": {"type": "keyword"},
            "node_key": {"type": "keyword"},
        }}})
    rng = make_rng("q2c")
    for ci in range(3):
        for space in ("spaceA", "spaceB"):
            v = synth_vector(centers[ci], 0.05, rng)
            es("PUT", f"/{VEC_INDEX}/_doc/{space}-n{ci}?routing={space}&refresh=true",
               json={"embedding": v.tolist(), "space_id": space, "node_key": f"{space}-n{ci}"})
    # routing -> 命中的分片数
    for space in ("spaceA", "spaceB"):
        sh = es("POST", f"/{VEC_INDEX}/_search_shards", params={"routing": space})
        print(f"_search_shards routing={space}: {len(sh['shards'])} shard(s), total={sh['total'] if 'total' in sh else len(sh['shards'])}")
    # kNN with routing=spaceA，查询向量靠近 center0
    q = synth_vector(centers[0], 0.02, make_rng("q2c-q"))
    r = knn_query(VEC_INDEX, q, k=3, routing="spaceA")
    hits = [(h["_source"]["node_key"], h.get("_shard"), round(h["_score"], 4)) for h in r.json()["hits"]["hits"]]
    print("kNN routing=spaceA hits (node_key, shard, score):", hits)
    assert all(h[0].startswith("spaceA") for h in hits), "routing 泄漏: 命中了别的 space!"
    r2 = knn_query(VEC_INDEX, q, k=3, routing="spaceB")
    hits2 = [(h["_source"]["node_key"], h.get("_shard"), round(h["_score"], 4)) for h in r2.json()["hits"]["hits"]]
    print("kNN routing=spaceB hits:", hits2)
    assert all(h[0].startswith("spaceB") for h in hits2), "routing 泄漏!"
    return True


def main():
    hits = part_a()
    assert "dave" in [str(h) for h in hits], "mixed index 全文查询未命中 dave"
    print("Q2a PASS: mixed index 全文查询命中")
    ok_b = part_b(cluster_centers())
    part_c(cluster_centers())
    hr("Q2 小结")
    print(f"(b) 同索引(JG索引)加 dense_vector + kNN: {'可行' if ok_b else '不可行'}")
    print("(c) 独立向量索引 + custom routing: 可行（见上方断言）")


if __name__ == "__main__":
    main()
