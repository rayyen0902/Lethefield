"""RMS 写入链地基（M2 提供最小写入原语，M15 写入链在此之上组装）。

约定：
- φ 初始化：n_last_touched = n_created，reinforce/conflict/neglect 三计数器置 0
  （开发文档 M15 写入链约定；衰减部分永不写回存储，见 M3）。
- temporal 边只由写入链按时间序建立，immutable——任何衰减/sweep 逻辑不得触碰。
- 绑定名避开 Groovy/Gremlin 保留字；long 型数字以字符串绑定传输、Groovy 侧 `as long`
  强转（gremlin_python 把 Python int 一律按 int32 序列化，超 2^31 客户端直接报错）。
"""

import json

from gremlin_python.driver.client import Client

from lethefield_rms.ff import DEFAULT_CONFIG as _FF_CONFIG
from lethefield_rms.ff import n_star_horizon
from lethefield_rms.schema import EDGE_LABELS

_CREATE_EVENT_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def v = t.addV()
    .property('node_key', nodeKey)
    .property('space_id', spaceId)
    .property('node_type', 'event')
    .property('content', contentText)
    .property('tau', new Date(tauMs as long))
    .property('ref_ex', refEx)
    .property('s', sVal as double)
    .property('n_created', nCreated as long)
    .property('n_last_touched', nCreated as long)
    .property('n_star_cached', nStar as long)
    .property('reinforce_count', 0)
    .property('conflict_count', 0)
    .property('neglect_count', 0)
    .next()
if (actorId != null) { v.property('agent_actor_id', actorId) }
if (attrsJson != null) { v.property('attrs', attrsJson) }
t.tx().commit()
'ok'
"""


_CREATE_ENTITY_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.addV()
    .property('node_key', nodeKey)
    .property('space_id', spaceId)
    .property('node_type', 'entity')
    .property('entity_key', entityKey)
    .next()
t.tx().commit()
'ok'
"""

_CREATE_EDGE_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def a = t.V().has('space_id', spaceId).has('node_key', fromKey).next()
def b = t.V().has('space_id', spaceId).has('node_key', toKey).next()
a.addEdge(edgeLabel, b)
t.tx().commit()
'ok'
"""


def _submit_ok(client: Client, script: str, bindings: dict) -> None:
    result = client.submit(script, bindings).all().result()
    if "ok" not in result:
        raise RuntimeError(f"写入未返回 ok：{result}")


def create_event_node(
    client: Client,
    gname: str,
    *,
    node_key: str,
    space_id: str,
    content: str,
    tau_ms: int,
    ref_ex: str,
    s: float,
    n_created: int,
    agent_actor_id: str | None = None,
    attrs: dict | None = None,
    n_star_cached: int | None = None,
) -> None:
    """创建 event 顶点。agent_actor_id / attrs 为 None 时不落对应属性。

    n_star_cached 为 None 时按 ff.n_star_horizon(s, n_created) 自动计算（M4 起）——
    检索前置粗筛 `WHERE n_star_cached > $n_now` 对 0 值节点会全灭召回，写入链
    默认必须给出正确视界。tau_ms / n_created / n_star_cached 以字符串绑定传输：
    gremlin_python 把 Python int 一律序列化为 int32，超 2^31 直接客户端报错；
    Groovy 侧 `as long` 兼容字符串。
    """
    if n_star_cached is None:
        n_star_cached = n_star_horizon(s, n_created, _FF_CONFIG.theta_base)
    _submit_ok(
        client,
        _CREATE_EVENT_SCRIPT,
        {
            "gname": gname,
            "nodeKey": node_key,
            "spaceId": space_id,
            "contentText": content,
            "tauMs": str(tau_ms),
            "refEx": ref_ex,
            "sVal": s,
            "nCreated": str(n_created),
            "nStar": str(n_star_cached),
            "actorId": agent_actor_id,
            "attrsJson": json.dumps(attrs, ensure_ascii=False) if attrs is not None else None,
        },
    )


def create_entity_node(client: Client, gname: str, *, entity_key: str, space_id: str) -> None:
    """创建 entity 顶点（node_type='entity'，node_key 取 'entity:{entity_key}'）。"""
    _submit_ok(
        client,
        _CREATE_ENTITY_SCRIPT,
        {
            "gname": gname,
            "nodeKey": f"entity:{entity_key}",
            "spaceId": space_id,
            "entityKey": entity_key,
        },
    )


def create_edge(
    client: Client,
    gname: str,
    *,
    space_id: str,
    from_key: str,
    to_key: str,
    label: str,
) -> None:
    """建立 from_key → to_key 的边。label 必须在 schema.EDGE_LABELS 内。

    temporal 边只由写入链按时间序建立，immutable——任何衰减/sweep 逻辑不得触碰
    （开发文档 §3：时序图不参与任何衰减/剪枝）。
    """
    if label not in EDGE_LABELS:
        raise ValueError(f"未知边标签 {label!r}，必须在 {EDGE_LABELS} 内")
    _submit_ok(
        client,
        _CREATE_EDGE_SCRIPT,
        {
            "gname": gname,
            "spaceId": space_id,
            "fromKey": from_key,
            "toKey": to_key,
            "edgeLabel": label,
        },
    )
