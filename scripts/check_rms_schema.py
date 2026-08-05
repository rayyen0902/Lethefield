"""M2 巡检：RMS 图 schema + rms_vectors mapping + ref_ex 抽样。

四层检查：
1. 静态：ensure_schema.groovy 与 schema.py 常量一致、无禁项
   （ids.authority.wait-time / drop，红线 4 与红线 5）
2. 图侧：指定图的 management 只读检查 17 属性键及类型、5 边标签、2 复合索引
3. 向量侧：rms_vectors 存在且 mapping 正确（node_key/space_id keyword、v dims>0）
4. ref_ex 抽样：event 顶点的 ref_ex 非空、为字符串、图内唯一。
   注意：ref_ex 的全链路追溯依赖 EX 事件存储（M10 才建），本脚本只覆盖 RMS 侧
   不变量；EX 侧 join 校验待 M10 补齐。

用法：uv run python scripts/check_rms_schema.py --graph <gname>
退出码：0 = 合规，1 = 存在失败项。
"""

import argparse
import sys

from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients import es_client, gremlin_client
from lethefield_rms.schema import (
    COMPOSITE_INDEXES,
    EDGE_LABELS,
    EXPECTED_PROPERTY_KEYS,
    ensure_schema_script,
)
from lethefield_rms.vectors import VECTORS_INDEX

# 索引状态接受 REGISTERED 或 ENABLED（复合索引建后异步启用）
ACCEPTED_INDEX_STATUS = {"REGISTERED", "ENABLED"}

_INSPECT_SCRIPT = """
def mgmt = ConfiguredGraphFactory.open(gname).openManagement()
def report = [:]
for (k in keyNames) {
    def pk = mgmt.getPropertyKey(k)
    report['key:' + k] = (pk == null) ? 'MISSING' : pk.dataType().getSimpleName()
}
for (e in edgeNames) {
    report['edge:' + e] = (mgmt.getEdgeLabel(e) == null) ? 'MISSING' : 'ok'
}
for (spec in indexSpecs) {
    def gi = mgmt.getGraphIndex(spec[0])
    if (gi == null) {
        report['index:' + spec[0]] = 'MISSING'
    } else {
        def status = gi.getIndexStatus(mgmt.getPropertyKey(spec[1])).toString()
        report['index:' + spec[0]] = status + (gi.isUnique() ? '|unique' : '|nonunique')
    }
}
mgmt.rollback()
report
"""

_SAMPLE_REF_EX_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('node_type', 'event').limit(n).project('nk', 'rx').by('node_key').by('ref_ex').toList()
"""


def static_check() -> list[str]:
    """groovy 资源与 schema 常量一致性 + 禁项检查。"""
    script = ensure_schema_script()
    failures = []
    for name in EXPECTED_PROPERTY_KEYS:
        if name not in script:
            failures.append(f"ensure_schema.groovy 缺少属性键 {name}")
    for label in EDGE_LABELS:
        if label not in script:
            failures.append(f"ensure_schema.groovy 缺少边标签 {label}")
    for index_name, _, _ in COMPOSITE_INDEXES:
        if index_name not in script:
            failures.append(f"ensure_schema.groovy 缺少索引 {index_name}")
    if "ids.authority.wait-time" in script:
        failures.append("ensure_schema.groovy 出现 ids.authority.wait-time（红线 4）")
    if "drop" in script.lower():
        failures.append("ensure_schema.groovy 出现 drop（schema 脚本不得含删除操作）")
    return failures


def inspect_graph(client: Client, gname: str) -> list[str]:
    """只读 mgmt 检查指定图的 schema 是否全量落地（mgmt.rollback，不做任何变更）。"""
    result = (
        client.submit(
            _INSPECT_SCRIPT,
            {
                "gname": gname,
                "keyNames": list(EXPECTED_PROPERTY_KEYS),
                "edgeNames": list(EDGE_LABELS),
                "indexSpecs": [[name, key] for name, key, _ in COMPOSITE_INDEXES],
            },
        )
        .all()
        .result()
    )
    # 服务端把返回 map 的每个 entry 作为独立结果项流回，先合并
    report = {k: v for item in result for k, v in item.items()}

    failures = []
    for name, type_name in EXPECTED_PROPERTY_KEYS.items():
        actual = report.get(f"key:{name}", "MISSING")
        if actual != type_name:
            failures.append(f"图 {gname} 属性键 {name} 类型为 {actual!r}，期望 {type_name!r}")
    for label in EDGE_LABELS:
        if report.get(f"edge:{label}") != "ok":
            failures.append(f"图 {gname} 缺少边标签 {label}")
    for index_name, _, unique in COMPOSITE_INDEXES:
        actual = report.get(f"index:{index_name}", "MISSING")
        status, _, unique_flag = actual.partition("|")
        if status not in ACCEPTED_INDEX_STATUS:
            failures.append(f"图 {gname} 索引 {index_name} 状态为 {actual!r}（未落地）")
        elif (unique_flag == "unique") != unique:
            failures.append(f"图 {gname} 索引 {index_name} 唯一性为 {unique_flag}，与定义不符")
    return failures


def check_vectors_index(es: Elasticsearch, index: str = VECTORS_INDEX) -> list[str]:
    """rms_vectors 存在且 mapping 正确。"""
    if not es.indices.exists(index=index):
        return [f"向量索引 {index} 不存在"]
    properties = es.indices.get_mapping(index=index)[index]["mappings"]["properties"]
    failures = []
    for field in ("node_key", "space_id"):
        actual = properties.get(field, {}).get("type")
        if actual != "keyword":
            failures.append(f"索引 {index} 字段 {field} 类型为 {actual!r}，期望 'keyword'")
    v = properties.get("v", {})
    if v.get("type") != "dense_vector":
        failures.append(f"索引 {index} 字段 v 类型为 {v.get('type')!r}，期望 'dense_vector'")
    elif not (v.get("dims") or 0) > 0:
        failures.append(f"索引 {index} 字段 v 的 dims 无效：{v.get('dims')!r}")
    return failures


def sample_ref_ex(client: Client, gname: str, limit: int = 100) -> list[str]:
    """抽样校验 event 顶点的 ref_ex：非空、为字符串、图内唯一。

    只覆盖 RMS 侧不变量；ref_ex → EX 原始事件的 join 校验待 M10（EX 事件存储）补齐。
    """
    rows = client.submit(_SAMPLE_REF_EX_SCRIPT, {"gname": gname, "n": limit}).all().result()
    if not rows:
        print(f"[skip] 图 {gname} 无 event 顶点，ref_ex 抽样跳过")
        return []
    failures = []
    seen: dict[str, str] = {}
    for row in rows:
        node_key, ref_ex = row["nk"], row["rx"]
        if not isinstance(ref_ex, str) or not ref_ex:
            failures.append(f"图 {gname} 顶点 {node_key} 的 ref_ex 非法：{ref_ex!r}")
        elif ref_ex in seen:
            failures.append(
                f"图 {gname} ref_ex {ref_ex!r} 重复（顶点 {seen[ref_ex]} 与 {node_key}）"
            )
        else:
            seen[ref_ex] = node_key
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="M2 RMS schema 巡检")
    parser.add_argument("--graph", required=True, help="要检查的图名（= keyspace 名）")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--index", default=VECTORS_INDEX)
    args = parser.parse_args()

    failures = static_check()

    client = gremlin_client()
    try:
        failures += inspect_graph(client, args.graph)
        failures += sample_ref_ex(client, args.graph)
    except Exception as exc:  # gremlin 不可达时不放行，巡检必须明确结论
        failures.append(f"图侧检查失败（gremlin 不可达？图 {args.graph} 不存在？）：{exc}")
    finally:
        client.close()

    try:
        failures += check_vectors_index(es_client(args.es_url), args.index)
    except Exception as exc:
        failures.append(f"向量索引检查失败（ES 不可达？）：{exc}")

    if failures:
        for failure in failures:
            print(f"[FAIL] {failure}", file=sys.stderr)
        return 1
    print(f"M2 巡检通过：图 {args.graph} schema 完整，索引 {args.index} mapping 正确")
    return 0


if __name__ == "__main__":
    sys.exit(main())
