import json

import pytest
from lethefield_clients import (
    CellInfo,
    SpaceMapping,
    SpaceStatus,
    SpaceType,
    Tier,
    WatermarkState,
    export_jsonl,
    restore_jsonl,
)


class _MemoryStore:
    """备份函数需要的最小具体接口（list_cells/list_space_mappings/register_*）。"""

    def __init__(self) -> None:
        self.cells: dict[str, CellInfo] = {}
        self.spaces: dict[str, SpaceMapping] = {}

    def list_cells(self, watermark_state=None):
        return sorted(self.cells.values(), key=lambda c: c.cell_id)

    def list_space_mappings(self):
        return sorted(self.spaces.values(), key=lambda m: m.space_id)

    def register_cell(self, cell):
        self.cells[cell.cell_id] = cell

    def register_space(self, mapping):
        self.spaces[mapping.space_id] = mapping


def _filled_store() -> _MemoryStore:
    store = _MemoryStore()
    store.register_cell(
        CellInfo(
            cell_id="cell-local",
            endpoints={"cassandra": "cassandra-cell", "es": "es-graph"},
            capacity={"keyspaces": 0.3},
            watermark_state=WatermarkState.FILLING,
        )
    )
    store.register_space(
        SpaceMapping(
            space_id="alpha",
            cell_id="cell-local",
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
            status=SpaceStatus.DESTROYING,  # 备份必须含非 active 映射
            tier=Tier.HOT,
            space_type=SpaceType.PROJECT,
        )
    )
    store.register_space(
        SpaceMapping(
            space_id="beta",
            cell_id="cell-local",
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
        )
    )
    return store


def test_export_restore_roundtrip(tmp_path):
    source = _filled_store()
    backup = tmp_path / "mapping.jsonl"
    assert export_jsonl(source, backup) == 3  # 1 cell + 2 spaces

    restored = _MemoryStore()
    assert restore_jsonl(restored, backup) == 3
    assert restored.cells == source.cells
    assert restored.spaces == source.spaces


def test_restore_idempotent(tmp_path):
    backup = tmp_path / "mapping.jsonl"
    export_jsonl(_filled_store(), backup)
    restored = _MemoryStore()
    restore_jsonl(restored, backup)
    restore_jsonl(restored, backup)  # 重复执行无副作用
    assert len(restored.spaces) == 2


def test_restore_rejects_unknown_version(tmp_path):
    bad = tmp_path / "bad.jsonl"
    bad.write_text(json.dumps({"v": 99, "kind": "space"}) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="版本"):
        restore_jsonl(_MemoryStore(), bad)


def test_export_empty_store(tmp_path):
    backup = tmp_path / "empty.jsonl"
    assert export_jsonl(_MemoryStore(), backup) == 0
    assert restore_jsonl(_MemoryStore(), backup) == 0
