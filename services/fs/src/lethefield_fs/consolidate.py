"""固化流程（M6 独立流程二）：置 consolidated_at + n_star_cached = LONG_MAX。

定案（升级确认）：consolidated_at 是第 17 个顶点属性（Date），存在即固化态——
s 锁定、跳过衰减计算与 sweep、检索时不再被 θ_effective 过滤；n_star_cached 置
LONG_MAX 让前置粗筛天然放行（粗筛不参与者实时计算，此用法合法）。
固化态 1.0 不解除（解除逻辑留待效果验证）。

幂等：已固化节点不重复写（hasNot('consolidated_at') 过滤），首次固化时间戳不被覆盖。
"""

import time

from gremlin_python.driver.client import Client

# Java Long 上限（与 ff._LONG_MAX 同值；固化节点的绝对遗忘视界 = +∞）
LONG_MAX = 2**63 - 1

_CONSOLIDATE_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def vs = t.V().has('space_id', spaceId).has('node_key', nodeKey)
    .hasNot('consolidated_at').toList()
if (!vs.isEmpty()) {
    def v = vs[0]
    v.property('consolidated_at', new Date(tsMs as long))
    v.property('n_star_cached', nStarMax as long)
}
t.tx().commit()
'ok'
"""


def consolidate_node(
    client: Client,
    gname: str,
    *,
    space_id: str,
    node_key: str,
    ts_ms: int | None = None,
) -> None:
    """固化单个节点（显式调用入口；阈值触发由 sweep 判定后经同一入口执行）。"""
    result = (
        client.submit(
            _CONSOLIDATE_SCRIPT,
            {
                "gname": gname,
                "spaceId": space_id,
                "nodeKey": node_key,
                "tsMs": str(ts_ms if ts_ms is not None else int(time.time() * 1000)),
                "nStarMax": str(LONG_MAX),
            },
        )
        .all()
        .result()
    )
    if "ok" not in result:
        raise RuntimeError(f"固化未返回 ok：{result}")
