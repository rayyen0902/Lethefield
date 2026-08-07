"""Q3: graph open 延迟实测（喂给目标架构课题）。

测量项:
  1) JanusGraph 容器重启 -> Gremlin 可服务时长
  2) ConfiguredGraphFactory 创建新图(create) / 打开(open) / 重开(reopen) 耗时分布
  3) 新图首次查询与后续查询延迟
  4) 每个动态图是否获得独立 Cassandra keyspace

用法: .venv/bin/python scripts/q3_graph_open.py
"""
import json
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "scripts")
from common import submit, wait_gremlin  # noqa: E402

N_GRAPHS = 5
PREFIX = "spikeq3"


def timed_submit(script, bindings=None):
    t0 = time.perf_counter()
    r = submit(script, bindings=bindings)
    return (time.perf_counter() - t0) * 1000.0, r


def restart_janusgraph():
    hr("Q3-1: JanusGraph 容器重启 -> 可服务时长")
    t0 = time.perf_counter()
    subprocess.run(["docker", "restart", "spike-janusgraph"], check=True,
                   capture_output=True)
    t_started = time.perf_counter()
    up_secs = wait_gremlin(timeout=900, interval=3)
    print(json.dumps({
        "docker_restart_cmd_s": round(t_started - t0, 2),
        "container_start_to_gremlin_service_s": round(up_secs, 2),
    }, indent=2))
    return up_secs


def try_create(name):
    """优先 ConfiguredGraphFactory.create(name)（模板配置）；失败则显式 createConfiguration。返回 (path, create_ms)"""
    ms, r = timed_submit(f"""
try {{
    org.janusgraph.core.ConfiguredGraphFactory.create("{name}")
    "template"
}} catch (Exception e) {{
    "template-failed: " + e.getClass().getSimpleName() + ": " + (e.getMessage() ?: "").take(200)
}}
""")
    if r and r[0] == "template":
        return "ConfiguredGraphFactory.create", ms
    print(f"  template create() unavailable for {name}: {r}")
    ms2, r2 = timed_submit(f"""
map = new HashMap<String, Object>()
map.put("storage.backend", "cql")
map.put("storage.hostname", "cassandra")
map.put("storage.cql.keyspace", "{name}")
map.put("index.search.backend", "elasticsearch")
map.put("index.search.hostname", "elasticsearch")
map.put("graph.graphname", "{name}")
org.janusgraph.core.ConfiguredGraphFactory.createConfiguration(new org.apache.commons.configuration2.MapConfiguration(map))
"explicit"
""")
    if not (r2 and r2[0] == "explicit"):
        raise RuntimeError(f"createConfiguration failed for {name}: {r2}")
    return "createConfiguration(explicit)", ms + ms2


def hr(t):
    print(f"\n===== {t} =====")


def summarize(label, xs):
    xs = sorted(xs)
    print(json.dumps({
        "metric": label,
        "n": len(xs),
        "min_ms": round(xs[0], 1),
        "p50_ms": round(statistics.median(xs), 1),
        "max_ms": round(xs[-1], 1),
        "mean_ms": round(statistics.mean(xs), 1),
    }))


def main():
    restart_s = restart_janusgraph()

    hr("Q3-2: ConfiguredGraphFactory 现有图")
    _, names = timed_submit("org.janusgraph.core.ConfiguredGraphFactory.getGraphNames()")
    print("existing graphs:", names)

    create_ms, open_ms, reopen_ms, first_q_ms, later_q_ms = [], [], [], [], []
    path_used = None
    for i in range(1, N_GRAPHS + 1):
        name = f"{PREFIX}{i}"
        hr(f"graph {name}")
        path, ms = try_create(name)
        path_used = path
        create_ms.append(ms)
        print(f"  create via {path}: {ms:.0f} ms")

        ms, _ = timed_submit(f'org.janusgraph.core.ConfiguredGraphFactory.open("{name}"); "ok"')
        open_ms.append(ms)
        print(f"  open (cold, right after create): {ms:.0f} ms")

        # close then reopen -> 温启动（keyspace 已存在，进程内缓存已清）
        timed_submit(f'org.janusgraph.core.ConfiguredGraphFactory.close("{name}"); "ok"')
        ms, _ = timed_submit(f'org.janusgraph.core.ConfiguredGraphFactory.open("{name}"); "ok"')
        reopen_ms.append(ms)
        print(f"  reopen (closed -> open): {ms:.0f} ms")

        # 首次查询 vs 后续查询（写1顶点+读count）
        ms, _ = timed_submit(f'g = org.janusgraph.core.ConfiguredGraphFactory.open("{name}").traversal(); g.addV("probe").property("k","v").next(); g.tx().commit(); "ok"')
        first_q_ms.append(ms)
        print(f"  first query (addV+commit): {ms:.0f} ms")
        for _ in range(3):
            ms, _ = timed_submit(f'g = org.janusgraph.core.ConfiguredGraphFactory.open("{name}").traversal(); g.V().count().next()')
            later_q_ms.append(ms)
        print(f"  subsequent count x3: last {ms:.1f} ms")

    hr("Q3-3: Cassandra keyspaces（验证每图独立 keyspace）")
    out = subprocess.run(
        ["docker", "exec", "spike-cassandra", "cqlsh", "-e", "describe keyspaces"],
        capture_output=True, text=True)
    print(out.stdout.strip())

    hr("Q3 汇总")
    print(f"container restart -> service: {restart_s:.1f}s")
    print(f"create path used: {path_used}")
    summarize("create(new graph, keyspace+tables init)", create_ms)
    summarize("open(cold)", open_ms)
    summarize("reopen(close->open)", reopen_ms)
    summarize("first query after open", first_q_ms)
    summarize("subsequent query", later_q_ms)


if __name__ == "__main__":
    main()
