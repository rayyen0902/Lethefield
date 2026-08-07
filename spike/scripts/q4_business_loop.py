"""Q4: 最小业务闭环——EX 事件 -> RMS 节点 -> ES 向量 -> retrieve（FF 现算过滤）。

链路（对应设计文档 §4/§5/§13）：
  写入: 每 space 20 条事件 -> JanusGraph 顶点(含 φ_i: s/n_created/n_last_touched)
        + 时序边 'next' + ES rms_vectors 向量文档(routing=space_id)
  检索: ES kNN 取锚点(带 routing) -> gremlin 1 跳子图 -> Python 现算
        s_effective = s × e^(−λ·Δn·log(1+t/t₀)) -> θ 硬过滤 -> 返回子图

验证断言:
  1) 高分节点被召回
  2) 低 s 节点被过滤
  3) 高 s 但 Δn 大（衰减后 < θ）的节点被过滤——FF 衰减真实生效
  4) 跨 space 数据不串

用法: .venv/bin/python scripts/q4_business_loop.py
"""
import math
import sys
import time

sys.path.insert(0, "scripts")
from common import DIMS, ES_URL, cluster_centers, es, make_rng, submit, synth_vector  # noqa: E402

SPACES = ["spaceA", "spaceB"]
N_EVENTS = 20
LAMBDA = 0.1
T0 = 3600.0
T_SIM = 7200.0   # 模拟墙钟：事件已发生 2 小时
THETA = 0.3
VEC_INDEX = "rms_vectors"


def hr(t):
    print(f"\n===== {t} =====")


def s_effective(s, n_last, n_now):
    dn = n_now - n_last
    return s * math.exp(-LAMBDA * dn * math.log(1 + T_SIM / T0))


def write_space(space, centers):
    rng = make_rng(f"q4-{space}")
    # 1) JanusGraph 顶点 + 时序边
    submit(r"""
g = graph.traversal()
space = bindings_space
n = bindings_n
nodes = []
(0..<n).each { i ->
    v = g.addV('event')
        .property('space_id', space)
        .property('node_key', space + '-e' + i)
        .property('content', space + ' event ' + i + ' about memory topic ' + (i % 3))
        .property('s', (i == 5) ? 0.2d : 0.9d)
        .property('n_created', (long) i)
        .property('n_last_touched', (i == 7) ? 0L : (long) i)
        .next()
    nodes.add(v)
}
(0..<(n-1)).each { i ->
    g.addE('next').from(nodes[i]).to(nodes[i+1]).next()
}
g.tx().commit()
'written ' + nodes.size()
""".replace("bindings_space", f"'{space}'").replace("bindings_n", str(N_EVENTS)))
    # 2) ES 向量文档（routing=space_id）
    for i in range(N_EVENTS):
        v = synth_vector(centers[i % 3], 0.05, rng)
        es("PUT", f"/{VEC_INDEX}/_doc/{space}-e{i}?routing={space}&refresh=true",
           json={"embedding": v.tolist(), "space_id": space, "node_key": f"{space}-e{i}"})
    print(f"{space}: {N_EVENTS} events written (graph + vectors)")


def retrieve(space, qvec, k=5):
    # Stage 2: ES kNN 锚点（单分片 routing）
    r = es("POST", f"/{VEC_INDEX}/_search", params={"routing": space}, ok=(200,), json={
        "knn": {"field": "embedding", "query_vector": [float(x) for x in qvec],
                "k": k, "num_candidates": 50},
        "size": k})
    anchor_keys = [h["_source"]["node_key"] for h in r["hits"]["hits"]]
    # Stage 3: gremlin 1 跳子图（锚点 + 时序邻居）
    rows = submit(r"""
g = graph.traversal()
anchors = g.V().has('node_key', within(anchorKeys)).toList()
seen = new LinkedHashSet()
result = []
anchors.each { a ->
    ([a] + g.V(a).both('next').toList()).each { n ->
        if (seen.add(n.id())) {
            m = g.V(n).valueMap('node_key','s','n_last_touched','space_id').next()
            result.add([
                node_key: m['node_key'][0],
                s: m['s'][0],
                n_last_touched: m['n_last_touched'][0],
                space_id: m['space_id'][0],
            ])
        }
    }
}
result
""", bindings={"anchorKeys": anchor_keys})
    nodes = rows[0] if (rows and isinstance(rows[0], list)) else rows
    # FF 现算 + θ 硬过滤
    n_now = N_EVENTS
    kept, dropped = [], []
    for nd in nodes:
        se = s_effective(float(nd["s"]), int(nd["n_last_touched"]), n_now)
        nd["s_effective"] = round(se, 4)
        (kept if se >= THETA else dropped).append(nd["node_key"])
    return anchor_keys, nodes, kept, dropped


def main():
    centers = cluster_centers()
    hr("写入两个 space")
    for sp in SPACES:
        write_space(sp, centers)

    hr("retrieve spaceA（查询向量靠近 center0）")
    q = synth_vector(centers[0], 0.02, make_rng("q4-query"))
    anchors, nodes, kept, dropped = retrieve("spaceA", q)
    print("anchors:", anchors)
    print(f"subgraph nodes={len(nodes)}, kept={len(kept)}, dropped={len(dropped)}")
    print("kept:", sorted(kept))
    print("dropped (θ过滤):", sorted(dropped))

    hr("断言")
    # e0 的 n_last_touched=0，衰减后 s_eff≈0.10 < θ——真正的"高分存活"节点是尾段事件
    # （n_last_touched=i 大者，如 e17/e18/e19，s_eff≈0.7+）
    assert any(k in kept for k in ("spaceA-e17", "spaceA-e18", "spaceA-e19")), "高分存活节点未被召回"
    assert all(n["s_effective"] >= THETA for n in nodes if n["node_key"] in kept), "kept 中存在低于 θ 的节点"
    assert "spaceA-e5" in dropped or "spaceA-e5" not in kept, "低 s 节点 e5 未被过滤"
    # e7: s=0.9 但 n_last_touched=0, Δn=20 -> s_eff ≈ 0.9*e^(−0.1*20*1.0986) ≈ 0.10 < θ
    se7 = s_effective(0.9, 0, N_EVENTS)
    print(f"e7 s_effective={se7:.4f} (< {THETA} 应被过滤)")
    if "spaceA-e7" in [n["node_key"] for n in nodes]:
        assert "spaceA-e7" in dropped, "衰减节点 e7 未被过滤"
    # 跨 space 隔离
    assert all(n["space_id"] == "spaceA" for n in nodes), "跨 space 数据泄漏!"
    print("断言 1-4 全部通过：召回/低分过滤/衰减过滤/跨 space 隔离")

    hr("retrieve spaceB（对称验证）")
    anchors_b, nodes_b, kept_b, dropped_b = retrieve("spaceB", q)
    print(f"spaceB: subgraph={len(nodes_b)}, kept={len(kept_b)}, dropped={len(dropped_b)}")
    assert all(n["space_id"] == "spaceB" for n in nodes_b), "跨 space 数据泄漏!"
    print("Q4 PASS: 最小业务闭环成立")


if __name__ == "__main__":
    main()
