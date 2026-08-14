"""M7 全保真档重放演练驱动（阶段 B 工单）：快照 → 销毁 RMS 侧 → 重放 → 字段级 diff。

用法（栈就绪、目标 space 有真实数据后）：
    uv run python scripts/rebuild_fidelity_drill.py snapshot --space S --out var/smoke/before.json
    uv run python scripts/rebuild_fidelity_drill.py destroy-rms --space S --confirm
    uv run python -m lethefield_rms.rebuild S          # 计时（RTO 实测）
    uv run python scripts/rebuild_fidelity_drill.py snapshot --space S --out var/smoke/after.json
    uv run python scripts/rebuild_fidelity_drill.py diff --space S --before B --after A

红线 1 合规：图遍历带 has('space_id')、ES 走 routing + space_id term 双机制、
入口 --space 收敛。destroy-rms 顺序对齐红线 5（close/removeConfiguration 先于
DROP KEYSPACE），且要求显式 --confirm。

diff 口径：consolidated_at 只比存在性（M7 定案：存在性保真、时间戳置执行时刻）；
rms_vectors 只登记销毁前基线——embedding 不可重放（红线 3），重放不负责还原
热节点向量，缺失如实报告。
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from cassandra import InvalidRequest
from lethefield_clients import (
    cassandra_cluster,
    es_client,
    gremlin_client,
    list_archived,
)

_VERTEX_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('space_id', sp).elementMap().toList()
"""

_EDGE_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('space_id', sp).outE('temporal', 'supersedes')
 .project('label', 'o', 'i')
 .by(label).by(outV().values('node_key')).by(inV().values('node_key'))
 .toList()
"""

_EVICT_SCRIPT = (
    "if (ConfiguredGraphFactory.getGraphNames().contains(gname)) { "
    "try { ConfiguredGraphFactory.close(gname) } catch (Exception ignored) {}; "
    "ConfiguredGraphFactory.removeConfiguration(gname) "
    "}; 'ok'"
)

# diff 比对的顶点字段（consolidated_at 单独按存在性比对）
_VERTEX_FIELDS = [
    "node_key",
    "content",
    "tau",
    "ref_ex",
    "s",
    "n_created",
    "n_last_touched",
    "n_star_cached",
    "reinforce_count",
    "conflict_count",
    "neglect_count",
    "agent_actor_id",
    "node_type",
]


def _norm(value):
    """图返回值归一化（Date → epoch ms ISO 可比口径；valueMap 单元素列表展开）。"""
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=__import__("datetime").UTC)
        return int(dt.timestamp() * 1000)
    if isinstance(value, list) and len(value) == 1:
        return _norm(value[0])
    return value


def cmd_snapshot(args) -> int:
    client = gremlin_client()
    es = es_client()
    cell = cassandra_cluster().connect()
    try:
        vertices_raw = (
            client.submit(_VERTEX_SCRIPT, {"gname": args.space, "sp": args.space}).all().result()
        )
        edges_raw = (
            client.submit(_EDGE_SCRIPT, {"gname": args.space, "sp": args.space}).all().result()
        )
        try:
            archives = list_archived(cell, args.space)
        except InvalidRequest:
            archives = []
    finally:
        client.close()
        cell.cluster.shutdown()

    vertices = {}
    for row in vertices_raw:
        props = {str(k): _norm(v) for k, v in dict(row).items() if str(k) not in ("id", "label")}
        key = props.get("node_key")
        props["consolidated"] = props.pop("consolidated_at", None) is not None
        vertices[key] = props
    edges = sorted(
        (str(e["label"]), str(e["o"]), str(e["i"])) for e in (dict(r) for r in edges_raw)
    )
    resp = es.search(
        index="rms_vectors",
        query={"term": {"space_id": args.space}},
        routing=args.space,
        size=1000,
        source=["node_key", "v"],
    )
    vectors = {
        hit["_source"]["node_key"]: {
            "dims": len(hit["_source"].get("v") or []),
            "sha256": hashlib.sha256(json.dumps(hit["_source"].get("v")).encode()).hexdigest()[:16],
        }
        for hit in resp["hits"]["hits"]
    }
    state = {
        "space": args.space,
        "vertices": vertices,
        "edges": [list(e) for e in edges],
        "vectors": vectors,
        "archives": [
            {"node_key": a["node_key"], "snapshot_props": a["snapshot"].get("props", {})}
            for a in archives
        ],
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str))
    print(
        f"[snapshot] {args.space}: 顶点 {len(vertices)}、边 {len(edges)}、"
        f"向量 {len(vectors)}、归档 {len(state['archives'])} → {args.out}"
    )
    return 0


def cmd_destroy(args) -> int:
    if not args.confirm:
        print("拒绝执行：销毁 RMS 侧需显式 --confirm", file=sys.stderr)
        return 2
    client = gremlin_client()
    cell = cassandra_cluster().connect()
    es = es_client()
    try:
        # 红线 5 顺序：先驱逐计算实例（close 容错 / removeConfiguration 严格），再 DROP
        client.submit(_EVICT_SCRIPT, {"gname": args.space}).all().result()
        cell.execute(f"DROP KEYSPACE IF EXISTS {args.space}", timeout=120)
        deleted = es.delete_by_query(
            index="rms_vectors",
            query={"term": {"space_id": args.space}},
            routing=args.space,
            refresh=True,
            conflicts="proceed",
        )
        names = client.submit("ConfiguredGraphFactory.getGraphNames()").all().result()
        assert args.space not in names, "图配置驱逐失败"
        print(
            f"[destroy] {args.space}: 图实例已驱逐 + keyspace 已 DROP + "
            f"rms_vectors 删除 {deleted.get('deleted')} 文档"
        )
    finally:
        client.close()
        cell.cluster.shutdown()
    return 0


def cmd_diff(args) -> int:
    before = json.loads(Path(args.before).read_text())
    after = json.loads(Path(args.after).read_text())
    report: dict = {"space": args.space, "categories": {}}

    # 顶点字段级
    vb, va = before["vertices"], after["vertices"]
    v_missing = sorted(set(vb) - set(va))
    v_extra = sorted(set(va) - set(vb))
    field_diffs = []
    for key in sorted(set(vb) & set(va)):
        for f in _VERTEX_FIELDS:
            x, y = vb[key].get(f), va[key].get(f)
            same = abs(x - y) < 1e-12 if isinstance(x, float) and isinstance(y, float) else x == y
            if not same:
                field_diffs.append({"node_key": key, "field": f, "before": x, "after": y})
        if vb[key].get("consolidated") != va[key].get("consolidated"):
            field_diffs.append(
                {
                    "node_key": key,
                    "field": "consolidated(存在性)",
                    "before": vb[key].get("consolidated"),
                    "after": va[key].get("consolidated"),
                }
            )
    report["categories"]["vertices"] = {
        "before": len(vb),
        "after": len(va),
        "missing": v_missing,
        "extra": v_extra,
        "field_diffs": field_diffs,
        "ok": not v_missing and not v_extra and not field_diffs,
    }

    # 边（label + 双端 node_key 多重集合比对）
    eb = sorted(map(tuple, before["edges"]))
    ea = sorted(map(tuple, after["edges"]))
    report["categories"]["edges"] = {
        "before": len(eb),
        "after": len(ea),
        "missing": [list(e) for e in sorted(set(eb) - set(ea))],
        "extra": [list(e) for e in sorted(set(ea) - set(eb))],
        "ok": eb == ea,
    }

    # 归档快照（字段级；v 不在快照 props 内，另见报告说明）
    ab = {a["node_key"]: a["snapshot_props"] for a in before["archives"]}
    aa = {a["node_key"]: a["snapshot_props"] for a in after["archives"]}
    report["categories"]["archives"] = {
        "before": len(ab),
        "after": len(aa),
        "ok": ab == aa,
        "diff": {
            k: {"before": ab.get(k), "after": aa.get(k)}
            for k in set(ab) | set(aa)
            if ab.get(k) != aa.get(k)
        },
    }

    # 向量：修订记录第 25 条——rms_vectors 不属重放范围（ES 快照运维前提），
    # 本类别只登记对照信息、不计入总体判定；图侧 node_key/ref_ex 保真即
    # "向量重关联语义可重建"的校验口径。
    kb, ka = set(before["vectors"]), set(after["vectors"])
    restored_same = sorted(
        k for k in kb & ka if before["vectors"][k]["sha256"] == after["vectors"][k]["sha256"]
    )
    report["categories"]["vectors"] = {
        "before": len(kb),
        "after": len(ka),
        "missing_after_rebuild": sorted(kb - ka),
        "restored_identical": restored_same,
        "informational": True,  # 不计入总体 ok（修订 25 边界）
        "note": "rms_vectors 不属重放范围；恢复 = ES 快照运维前提（runbook 已落地）",
    }

    report["ok"] = all(c["ok"] for c in report["categories"].values() if not c.get("informational"))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(
        json.dumps(
            {
                k: ("informational" if v.get("informational") else v["ok"])
                for k, v in report["categories"].items()
            },
            ensure_ascii=False,
        )
    )
    print(f"[diff] → {args.out}  总体 {'OK' if report['ok'] else '有差异'}")
    return 0 if report["ok"] else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rebuild_fidelity_drill", description="M7 重放演练")
    sub = parser.add_subparsers(dest="command", required=True)

    p_snap = sub.add_parser("snapshot", help="RMS 侧字段级快照")
    p_snap.add_argument("--space", required=True)
    p_snap.add_argument("--out", required=True)

    p_destroy = sub.add_parser("destroy-rms", help="销毁 RMS 侧（图 + keyspace + 向量文档）")
    p_destroy.add_argument("--space", required=True)
    p_destroy.add_argument("--confirm", action="store_true")

    p_diff = sub.add_parser("diff", help="前后快照字段级比对")
    p_diff.add_argument("--space", required=True)
    p_diff.add_argument("--before", required=True)
    p_diff.add_argument("--after", required=True)
    p_diff.add_argument("--out", required=True)

    args = parser.parse_args(argv)
    if args.command == "snapshot":
        return cmd_snapshot(args)
    if args.command == "destroy-rms":
        return cmd_destroy(args)
    if args.command == "diff":
        return cmd_diff(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
