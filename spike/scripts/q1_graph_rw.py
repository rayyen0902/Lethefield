"""Q1: 单节点起栈 + JanusGraph 图读写最小验证。

用法: .venv/bin/python scripts/q1_graph_rw.py
"""
import json
import sys
import time

from gremlin_python.driver import client as gclient
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

sys.path.insert(0, "scripts")
from common import submit, wait_gremlin  # noqa: E402


def main():
    t0 = time.time()
    up_secs = wait_gremlin()
    print(f"[Q1] gremlin server ready after wait: {up_secs:.1f}s (session time)")

    # 1) 探测 server 端绑定（graph / traversal source 名称）
    try:
        names = submit("binding.variables.keySet().sort()")
        print(f"[Q1] server bindings: {names}")
    except Exception as e:  # noqa: BLE001
        print(f"[Q1] binding introspection failed (non-fatal): {e}")

    # 2) 找一个可用的 traversal source 绑定
    ts_name = None
    for cand in ("g", "graph_traversal", "graph"):
        try:
            n = submit(f"{cand}.V().limit(1).count().next()")
            ts_name = cand
            print(f"[Q1] traversal source '{cand}' works, V count probe = {n}")
            break
        except Exception:  # noqa: BLE001
            continue
    if ts_name is None:
        raise RuntimeError("no usable traversal source binding found")

    # 3) 清空并写入顶点/边（脚本方式，含属性）
    submit("g = graph.traversal(); g.V().drop().iterate()" if ts_name != "g"
           else "g.V().drop().iterate()")
    write_script = """
g = %s
alice = g.addV('person').property('name','alice').property('age', 30).property('content','alice likes graph databases and memory systems').next()
bob   = g.addV('person').property('name','bob').property('age', 28).property('content','bob studies vector search and embeddings').next()
carol = g.addV('person').property('name','carol').property('age', 35).property('content','carol builds retrieval augmented generation pipelines').next()
g.addE('knows').from(alice).to(bob).property('since', 2020).next()
g.addE('knows').from(bob).to(carol).property('since', 2022).next()
g.tx().commit()
'written'
""" % ("graph.traversal()" if ts_name != "g" else "g")
    print(f"[Q1] write: {submit(write_script)}")

    # 4) 读回：顶点数、边数、属性、遍历
    counts = submit("g = %s; [v: g.V().count().next(), e: g.E().count().next()]"
                    % ("graph.traversal()" if ts_name != "g" else "g"))
    print(f"[Q1] counts: {counts}")
    people = submit("g = %s; g.V().hasLabel('person').order().by('name').valueMap('name','age','content').toList()"
                    % ("graph.traversal()" if ts_name != "g" else "g"))
    print(f"[Q1] vertices: {json.dumps(people[0], ensure_ascii=False, default=str)}")
    hops = submit("g = %s; g.V().has('name','alice').out('knows').out('knows').values('name').toList()"
                  % ("graph.traversal()" if ts_name != "g" else "g"))
    print(f"[Q1] alice ->2hop knows-> : {hops}")

    # 5) 再用标准 DriverRemoteConnection (bytecode) API 验证一次
    conn = DriverRemoteConnection("ws://localhost:8182/gremlin", ts_name if ts_name != "graph" else "graph")
    g = traversal().with_remote(conn)
    n = g.V().count().next()
    names = g.V().has_label("person").values("name").to_list()
    conn.close()
    print(f"[Q1] bytecode API: V count = {n}, names = {sorted(map(str, names))}")

    print(f"[Q1] PASS in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
