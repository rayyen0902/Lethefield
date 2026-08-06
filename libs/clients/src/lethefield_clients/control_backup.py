"""映射表备份/导出（M9，1.0 验收硬指标）。

设计文档 §17.2：映射表丢失 = 全部 space 失联（数据还在、入口丢失），
必须有备份与导出机制。本模块提供 JSONL 全量导出/恢复：

- 导出：cells + spaces 全量（含 migrating/destroying 中的——list_spaces 只给
  active，备份不能用），每行一个 JSON 对象，`"v": 1` 版本字段预留演进；
- 恢复：按主键覆盖写（Cassandra INSERT = upsert），幂等，可重复执行；
- 灾难恢复演练：export → 清空控制面表 → restore → 映射与 sweep 枚举复原
  （tests/integration/test_m9_cell_scheduler.py 验收项 1）。
"""

import json
from pathlib import Path

from lethefield_clients.control_plane import (
    CellInfo,
    MappingTableControlPlaneStore,
    SpaceMapping,
    SpaceStatus,
    SpaceType,
    Tier,
    WatermarkState,
)

FORMAT_VERSION = 1


def export_jsonl(store: MappingTableControlPlaneStore, path: str | Path) -> int:
    """导出 cells + spaces 全量到 JSONL 文件，返回总行数。"""
    lines = [_cell_to_json(cell) for cell in store.list_cells()]
    lines += [_mapping_to_json(m) for m in store.list_space_mappings()]
    Path(path).write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")
    return len(lines)


def restore_jsonl(store: MappingTableControlPlaneStore, path: str | Path) -> int:
    """从 JSONL 恢复（覆盖写，幂等），返回恢复行数。"""
    count = 0
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        record = json.loads(raw)
        if record.get("v") != FORMAT_VERSION:
            raise ValueError(f"备份格式版本不支持：{record.get('v')!r}（当前 {FORMAT_VERSION}）")
        if record["kind"] == "cell":
            store.register_cell(_cell_from_json(record))
        elif record["kind"] == "space":
            store.register_space(_mapping_from_json(record))
        else:
            raise ValueError(f"未知备份记录类型：{record['kind']!r}")
        count += 1
    return count


def _cell_to_json(cell: CellInfo) -> str:
    return json.dumps(
        {
            "v": FORMAT_VERSION,
            "kind": "cell",
            "cell_id": cell.cell_id,
            "endpoints": dict(cell.endpoints),
            "capacity": dict(cell.capacity),
            "watermark_state": cell.watermark_state.value,
        },
        sort_keys=True,
    )


def _mapping_to_json(mapping: SpaceMapping) -> str:
    return json.dumps(
        {
            "v": FORMAT_VERSION,
            "kind": "space",
            "space_id": mapping.space_id,
            "cell_id": mapping.cell_id,
            "ex_cluster_id": mapping.ex_cluster_id,
            "pulsar_cluster_id": mapping.pulsar_cluster_id,
            "status": mapping.status.value,
            "tier": mapping.tier.value,
            "space_type": mapping.space_type.value if mapping.space_type is not None else None,
        },
        sort_keys=True,
    )


def _cell_from_json(record: dict) -> CellInfo:
    return CellInfo(
        cell_id=record["cell_id"],
        endpoints=dict(record.get("endpoints") or {}),
        capacity=dict(record.get("capacity") or {}),
        watermark_state=WatermarkState(record["watermark_state"]),
    )


def _mapping_from_json(record: dict) -> SpaceMapping:
    space_type = record.get("space_type")
    return SpaceMapping(
        space_id=record["space_id"],
        cell_id=record["cell_id"],
        ex_cluster_id=record["ex_cluster_id"],
        pulsar_cluster_id=record["pulsar_cluster_id"],
        status=SpaceStatus(record["status"]),
        tier=Tier(record["tier"]),
        space_type=SpaceType(space_type) if space_type is not None else None,
    )
