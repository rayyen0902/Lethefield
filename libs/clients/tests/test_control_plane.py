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


def test_list_spaces_static(store):
    assert store.list_spaces() == []
    store.register_space(_mapping("space-b"))
    store.register_space(_mapping("space-a"))
    assert store.list_spaces() == ["space-a", "space-b"]


class _FakeExSession:
    def __init__(self, keyspaces: list[str]) -> None:
        self._rows = [type("Row", (), {"keyspace_name": k}) for k in keyspaces]

    def execute(self, query: str):
        assert "system_schema.keyspaces" in query
        return self._rows


def test_ex_keyspace_store_derives_spaces(store):
    from lethefield_clients import ExKeyspaceControlPlaneStore

    ex = ExKeyspaceControlPlaneStore(
        _FakeExSession(["ex_alpha", "ex_beta", "system_schema", "rms_graph"]),
        delegate=store,
    )
    assert ex.list_spaces() == ["alpha", "beta"]
    # 非枚举方法委托给 delegate
    ex.register_space(_mapping("alpha"))
    assert ex.get_space_mapping("alpha").cell_id == "cell-local"
    assert ex.list_cells() == store.list_cells()
