"""水位状态机与选 Cell 逻辑单测（state_of 纯函数 + select_cell/refresh_cell fake store）。"""

import pytest
from lethefield_clients import CellInfo, WatermarkState
from lethefield_scheduler.config import SchedulerConfig
from lethefield_scheduler.watermark import (
    NoOpenCellError,
    refresh_cell,
    select_cell,
    state_of,
)

CONFIG = SchedulerConfig()  # 0.7 / 0.9 初值


@pytest.mark.parametrize(
    ("capacity", "expected"),
    [
        ({}, WatermarkState.OPEN),
        ({"keyspaces": 0.0}, WatermarkState.OPEN),
        ({"keyspaces": 0.69, "es_shards": 0.1}, WatermarkState.OPEN),
        ({"keyspaces": 0.7}, WatermarkState.FILLING),  # 阈值含边界：≥0.7
        ({"keyspaces": 0.1, "disk": 0.85}, WatermarkState.FILLING),  # 任一维触发
        ({"keyspaces": 0.9}, WatermarkState.CLOSED),  # ≥0.9
        ({"keyspaces": 0.89, "heap": 0.95}, WatermarkState.CLOSED),
    ],
)
def test_state_of(capacity, expected):
    assert state_of(capacity, CONFIG) == expected


class _FakeStore:
    def __init__(self, cells: list[CellInfo]) -> None:
        self._cells = {c.cell_id: c for c in cells}
        self.updated: list[tuple[str, dict, WatermarkState]] = []

    def list_cells(self, watermark_state=None):
        cells = sorted(self._cells.values(), key=lambda c: c.cell_id)
        if watermark_state is not None:
            cells = [c for c in cells if c.watermark_state == watermark_state]
        return cells

    def get_cell(self, cell_id):
        return self._cells[cell_id]

    def update_cell_watermark(self, cell_id, capacity, state):
        cell = self._cells[cell_id]
        self._cells[cell_id] = CellInfo(
            cell_id=cell_id, endpoints=cell.endpoints, capacity=capacity, watermark_state=state
        )
        self.updated.append((cell_id, capacity, state))


def _cell(cell_id, state=WatermarkState.OPEN, capacity=None) -> CellInfo:
    return CellInfo(cell_id=cell_id, capacity=capacity or {}, watermark_state=state)


def test_select_cell_picks_lowest_watermark():
    store = _FakeStore(
        [
            _cell("cell-a", capacity={"keyspaces": 0.5}),
            _cell("cell-b", capacity={"keyspaces": 0.1, "es_shards": 0.2}),
            _cell("cell-c", capacity={"keyspaces": 0.4}),
        ]
    )
    assert select_cell(store).cell_id == "cell-b"


def test_select_cell_skips_non_open():
    store = _FakeStore(
        [
            _cell("cell-a", state=WatermarkState.FILLING, capacity={"keyspaces": 0.01}),
            _cell("cell-b", capacity={"keyspaces": 0.6}),
        ]
    )
    assert select_cell(store).cell_id == "cell-b"


def test_select_cell_no_open_raises():
    store = _FakeStore(
        [
            _cell("cell-a", state=WatermarkState.FILLING),
            _cell("cell-b", state=WatermarkState.CLOSED),
        ]
    )
    with pytest.raises(NoOpenCellError):
        select_cell(store)


def test_refresh_cell_persists_transition():
    store = _FakeStore([_cell("cell-a")])
    cell = refresh_cell(store, "cell-a", probe={"keyspaces": 0.75})
    assert cell.watermark_state == WatermarkState.FILLING
    assert store.updated == [("cell-a", {"keyspaces": 0.75}, WatermarkState.FILLING)]
    assert store.get_cell("cell-a").capacity == {"keyspaces": 0.75}
