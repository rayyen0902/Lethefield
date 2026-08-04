import pytest
from lethefield_clients import (
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
    StaticControlPlaneStore,
    Tier,
    WatermarkState,
)


@pytest.fixture
def store():
    return StaticControlPlaneStore.local()


def _mapping(space_id: str, cell_id: str = "cell-local") -> SpaceMapping:
    return SpaceMapping(
        space_id=space_id,
        cell_id=cell_id,
        ex_cluster_id="ex-local",
        pulsar_cluster_id="pulsar-local",
        tier=Tier.COLD,
    )


def test_register_and_lookup(store):
    store.register_space(_mapping("space-1"))
    mapping = store.get_space_mapping("space-1")
    assert mapping.cell_id == "cell-local"
    assert mapping.status == SpaceStatus.ACTIVE


def test_unknown_space_raises(store):
    with pytest.raises(SpaceNotFoundError):
        store.get_space_mapping("nope")


def test_register_to_foreign_cell_rejected(store):
    with pytest.raises(ValueError, match="单一 Cell"):
        store.register_space(_mapping("space-x", cell_id="cell-elsewhere"))


def test_update_status(store):
    store.register_space(_mapping("space-1"))
    store.update_space_status("space-1", SpaceStatus.DESTROYING)
    assert store.get_space_mapping("space-1").status == SpaceStatus.DESTROYING


def test_get_cell(store):
    cell = store.get_cell("cell-local")
    assert cell.watermark_state == WatermarkState.OPEN
    assert "cassandra" in cell.endpoints
    with pytest.raises(KeyError):
        store.get_cell("cell-nope")


def test_list_cells_filter(store):
    assert len(store.list_cells()) == 1
    assert len(store.list_cells(WatermarkState.OPEN)) == 1
    assert store.list_cells(WatermarkState.CLOSED) == []
