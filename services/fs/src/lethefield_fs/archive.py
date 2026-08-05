"""归档流程（M6 独立流程一）：热图顶点 → 冷存副本 + ES 向量清理 + 顶点移除。

定案载体（升级确认）：归档副本写本 space 自己的 RMS keyspace（= 图名）内专用表
`archived_nodes`，直写 CQL、不经 JanusGraph；表名与 CQL 全部封装在
libs/clients（lethefield_clients.archive），本模块不裸写 CQL。

快照内容 = 节点全字段 + 图邻接（出入边 label + 对端 node_key）；EX 原始记录
不受影响，ref_ex 保证可溯源重建。M7 重放重建经 list_archived 读回。
"""

from datetime import UTC, datetime

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients import ensure_archive_table, write_archive
from lethefield_rms.vectors import delete_vector

# 归档顶点全字段（node_type 恒为 event，不存；consolidated_at 不会出现在归档节点上——
# 固化节点永不满足归档资格）
_PROPS_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('space_id', spaceId).has('node_key', nodeKey)
    .valueMap('content', 'tau', 'agent_actor_id', 'attrs', 'ref_ex', 's',
              'n_created', 'n_last_touched', 'n_star_cached',
              'reinforce_count', 'conflict_count', 'neglect_count')
    .next()
"""

_EDGES_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def edges = t.V().has('space_id', spaceId).has('node_key', nodeKey)
    .bothE()
    .project('label', 'out_key', 'in_key')
    .by(label)
    .by(outV().values('node_key'))
    .by(inV().values('node_key'))
    .toList()
['edges': edges]
"""

_DROP_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('space_id', spaceId).has('node_key', nodeKey).drop().iterate()
t.tx().commit()
'ok'
"""


def _merge_entries(result: list) -> dict:
    """valueMap 结果按 entry 逐个流回（JanusGraph 服务端流式语义），先合并再取单元素值。"""
    merged = {k: v for item in result for k, v in item.items()}
    return {k: v[0] for k, v in merged.items()}


def build_snapshot(props: dict, edges: list[dict]) -> dict:
    """构造 JSON 可序列化的归档快照（tau 等 Date 转 epoch 毫秒，保证重建可解析）。

    gremlin_python 反序列化的 Date 是 naive datetime（UTC 语义）——
    直接 .timestamp() 会被当本地时间，必须先按 UTC 解释。
    """
    normalized = {}
    for key, value in props.items():
        if isinstance(value, datetime):
            aware = value if value.tzinfo else value.replace(tzinfo=UTC)
            normalized[key] = int(aware.timestamp() * 1000)
        else:
            normalized[key] = value
    return {"props": normalized, "edges": edges}


def archive_node(
    client: Client,
    cell_session: Session,
    es: Elasticsearch,
    *,
    gname: str,
    space_id: str,
    node_key: str,
) -> dict:
    """归档单个节点：快照 → 写 archived_nodes → 删 ES 向量 → 热图移除。返回快照。"""
    bindings = {"gname": gname, "spaceId": space_id, "nodeKey": node_key}
    props = _merge_entries(client.submit(_PROPS_SCRIPT, bindings).all().result())
    if not props:
        raise KeyError(f"节点不存在：space={space_id} node_key={node_key}")
    edges_payload = {
        k: v
        for item in client.submit(_EDGES_SCRIPT, bindings).all().result()
        for k, v in item.items()
    }
    snapshot = build_snapshot(props, edges_payload.get("edges", []))

    ensure_archive_table(cell_session, gname)
    write_archive(cell_session, gname, node_key=node_key, snapshot=snapshot)
    # 先落冷存副本，再清检索入口与热图——任何一步失败残留都可由下一轮 sweep 重试
    delete_vector(es, space_id=space_id, node_key=node_key)
    result = client.submit(_DROP_SCRIPT, bindings).all().result()
    if "ok" not in result:
        raise RuntimeError(f"归档移除顶点未返回 ok：{result}")
    return snapshot
