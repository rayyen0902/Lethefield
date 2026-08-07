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


def test_space_type_annotation(store):
    """M8：space_type 是 SpaceMapping 的可选产品/运营标注，核心服务不消费。"""
    from lethefield_clients import SpaceType

    assert _mapping("plain").space_type is None  # 默认 None，向后兼容
    typed = SpaceMapping(
        space_id="typed",
        cell_id="cell-local",
        ex_cluster_id="ex-local",
        pulsar_cluster_id="pulsar-local",
        space_type=SpaceType.PROJECT,
    )
    store.register_space(typed)
    assert store.get_space_mapping("typed").space_type == SpaceType.PROJECT
    # 状态更新不丢标注
    store.update_space_status("typed", SpaceStatus.MIGRATING)
    assert store.get_space_mapping("typed").space_type == SpaceType.PROJECT


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


def test_mapping_row_roundtrip():
    """映射行 serde 单测：_row_to_mapping/_row_to_cell 与 Cassandra 行形状对齐。"""
    from lethefield_clients.control_plane import _row_to_cell, _row_to_mapping

    row = type(
        "Row",
        (),
        {
            "space_id": "s1",
            "cell_id": "cell-local",
            "ex_cluster_id": "ex-local",
            "pulsar_cluster_id": "pulsar-local",
            "status": "migrating",
            "tier": "hot",
            "space_type": None,
        },
    )()
    mapping = _row_to_mapping(row)
    assert mapping.status == SpaceStatus.MIGRATING
    assert mapping.tier == Tier.HOT
    assert mapping.space_type is None

    cell_row = type(
        "Row",
        (),
        {
            "cell_id": "cell-local",
            "endpoints": {"cassandra": "cassandra-cell"},
            "capacity": {"keyspaces": 0.5},
            "watermark_state": "filling",
        },
    )()
    cell = _row_to_cell(cell_row)
    assert cell.watermark_state == WatermarkState.FILLING
    assert cell.capacity == {"keyspaces": 0.5}

    empty_cell_row = type(
        "Row", (), {"cell_id": "c", "endpoints": None, "capacity": None, "watermark_state": "open"}
    )()
    assert _row_to_cell(empty_cell_row).endpoints == {}


# ---------------------------------------------------------------- M10 迁移切映射


class _FakeSession:
    """MappingTable 单测用 session：记录 UPDATE 语句 + get_space_mapping 查询。"""

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        if "SELECT" in statement and "spaces" in statement:
            row = type(
                "Row",
                (),
                {
                    "space_id": "sp1",
                    "cell_id": "cell-1",
                    "ex_cluster_id": "ex-local",
                    "pulsar_cluster_id": "p",
                    "status": "active",
                    "tier": "cold",
                    "space_type": None,
                },
            )()
            return type("Rs", (), {"one": lambda self: row})()
        return type("Rs", (), {"one": lambda self: None})()


def test_update_space_cell():
    """M10：迁移切映射只改归属字段（cell_id/ex_cluster_id），不动 status/tier。"""
    from lethefield_clients import MappingTableControlPlaneStore

    session = _FakeSession()
    store = MappingTableControlPlaneStore(session)
    store.update_space_cell("sp1", "cell-2", "ex-local")
    update = [s for s in session.statements if s[0].startswith("UPDATE")]
    assert len(update) == 1
    statement, params = update[0]
    assert "SET cell_id = %s, ex_cluster_id = %s" in statement
    assert "status" not in statement.split("SET")[1]  # 状态翻转由迁移流水线显式执行
    assert params == ("cell-2", "ex-local", "sp1")
