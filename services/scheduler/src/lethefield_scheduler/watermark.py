"""水位制调度（设计文档 §17.3：刻意保持简单）。

三档状态由四维水位推导：keyspace 数 / ES 分片数 / 磁盘 / 堆压力。
单节点起步形态 disk/heap 无便捷探针，默认 0.0 且 probe 可注入——
模拟负载与多维度标定走注入路径（M9 验收"模拟负载下正确触发"即此）。

明确不做（§17.3 已否决）：自动再平衡、动态打分装箱。
"""

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from lethefield_clients import (
    CONTROL_KEYSPACE,
    CellInfo,
    MappingTableControlPlaneStore,
    WatermarkState,
)

from lethefield_scheduler.config import DEFAULT_CONFIG, SchedulerConfig

# 水位维度（四维，§17.3）
DIM_KEYSPACES = "keyspaces"
DIM_ES_SHARDS = "es_shards"
DIM_DISK = "disk"
DIM_HEAP = "heap"


class NoOpenCellError(RuntimeError):
    """没有可分配的 open Cell（filling/closed 只出不进）。"""


def state_of(
    capacity: dict[str, float], config: SchedulerConfig = DEFAULT_CONFIG
) -> WatermarkState:
    """水位字典 → 三档状态（纯函数）：任一维 ≥closed → closed；≥filling → filling。"""
    if any(v >= config.closed_threshold for v in capacity.values()):
        return WatermarkState.CLOSED
    if any(v >= config.filling_threshold for v in capacity.values()):
        return WatermarkState.FILLING
    return WatermarkState.OPEN


def probe_capacity(
    cell_session: Session,
    es: Elasticsearch,
    config: SchedulerConfig = DEFAULT_CONFIG,
) -> dict[str, float]:
    """实测可测维度：cell Cassandra 图 keyspace 数 / ES 分片数。

    图 keyspace = 全部 keyspace 排除 system_* 与控制面 keyspace（映射表不占图容量）。
    disk/heap 单节点形态无探针，置 0.0——阈值维度已接线，标定后补探针。
    """
    rows = cell_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    keyspaces = sum(
        1
        for row in rows
        if not row.keyspace_name.startswith("system") and row.keyspace_name != CONTROL_KEYSPACE
    )
    shards = len(es.cat.shards(format="json"))
    return {
        DIM_KEYSPACES: keyspaces / config.keyspace_cap,
        DIM_ES_SHARDS: shards / config.es_shard_cap,
        DIM_DISK: 0.0,
        DIM_HEAP: 0.0,
    }


def refresh_cell(
    store: MappingTableControlPlaneStore,
    cell_id: str,
    *,
    probe: dict[str, float] | None = None,
    cell_session: Session | None = None,
    es: Elasticsearch | None = None,
    config: SchedulerConfig = DEFAULT_CONFIG,
) -> CellInfo:
    """探测（或注入）水位并持久化 capacity + 状态转换，返回更新后的 CellInfo。

    probe 缺省时需给 cell_session + es 走实测；注入优先（测试/模拟负载）。
    """
    capacity = probe if probe is not None else probe_capacity(cell_session, es, config)
    state = state_of(capacity, config)
    store.update_cell_watermark(cell_id, capacity, state)
    return store.get_cell(cell_id)


def select_cell(
    store: MappingTableControlPlaneStore,
) -> CellInfo:
    """选水位最低的 open Cell（最大维水位最小者）；无 open Cell 抛 NoOpenCellError。"""
    open_cells = store.list_cells(WatermarkState.OPEN)
    if not open_cells:
        raise NoOpenCellError("没有 open 状态的 Cell，停止新分配（先扩容再开通）")

    def max_dim(cell: CellInfo) -> float:
        return max(cell.capacity.values(), default=0.0)

    return min(open_cells, key=lambda c: (max_dim(c), c.cell_id))
