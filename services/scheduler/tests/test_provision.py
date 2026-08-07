"""开通流水线单测：步骤顺序、先存储后注册、失败回滚（fake 依赖注入）。

验收对应（开发文档 §10）：存储步骤失败时注册不执行，且已完成的存储资源被回滚清理。
"""

import pytest
from lethefield_clients import (
    CellInfo,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
    Tier,
    WatermarkState,
)
from lethefield_scheduler import provision as provision_mod
from lethefield_scheduler.provision import (
    ProvisionDeps,
    ProvisionError,
    ProvisionRollbackError,
    provision_space,
)
from lethefield_scheduler.watermark import NoOpenCellError


class _FakeStore:
    def __init__(self, cells=None, fail_register=False, events: list[str] | None = None) -> None:
        self._cells = (
            cells
            if cells is not None
            else [
                CellInfo(
                    cell_id="cell-local",
                    endpoints={"cassandra": "cassandra-cell", "es": "es-graph"},
                )
            ]
        )
        self._spaces: dict[str, SpaceMapping] = {}
        self._fail_register = fail_register
        self.events: list[str] = events if events is not None else []

    def list_cells(self, watermark_state=None):
        if watermark_state is None:
            return self._cells
        return [c for c in self._cells if c.watermark_state == watermark_state]

    def get_space_mapping(self, space_id):
        try:
            return self._spaces[space_id]
        except KeyError:
            raise SpaceNotFoundError(space_id) from None

    def register_space(self, mapping):
        self.events.append("register")
        if self._fail_register:
            raise RuntimeError("store write failed")
        self._spaces[mapping.space_id] = mapping


class _FakeGremlin:
    def __init__(self) -> None:
        self.scripts: list[str] = []

    class _Result:
        def all(self):
            return self

        def result(self):
            return ["ok"]

    def submit(self, script, bindings=None):
        self.scripts.append(script)
        return self._Result()


class _FakeSession:
    def __init__(self) -> None:
        self.cql: list[str] = []

    def execute(self, statement, parameters=None, **kwargs):
        self.cql.append(statement)


@pytest.fixture
def events(monkeypatch):
    """把 EX/Pulsar/图三步替换为事件记录器，返回 (events, rollback 事件同列)。"""
    log: list[str] = []
    monkeypatch.setattr(provision_mod, "ensure_ex_keyspace", lambda session, sid: log.append("ex"))
    monkeypatch.setattr(
        provision_mod.pulsar_admin,
        "ensure_namespace",
        lambda url, tenant, ns: log.append("pulsar"),
    )
    # namespace 策略（retention/backlog quota）：本套件关心编排顺序，策略调用打平为 no-op
    monkeypatch.setattr(provision_mod.pulsar_admin, "set_retention", lambda *a, **k: None)
    monkeypatch.setattr(provision_mod.pulsar_admin, "set_backlog_quota", lambda *a, **k: None)
    monkeypatch.setattr(
        provision_mod,
        "ensure_graph_schema",
        lambda client, gname, backend_props=None: log.append("graph"),
    )
    monkeypatch.setattr(
        provision_mod.pulsar_admin,
        "delete_namespace",
        lambda url, tenant, ns: log.append("rb:pulsar"),
    )
    monkeypatch.setattr(provision_mod, "_rollback_graph", lambda deps, sid: log.append("rb:graph"))
    return log


def _deps(store) -> ProvisionDeps:
    return ProvisionDeps(
        store=store, gremlin=_FakeGremlin(), ex_session=_FakeSession(), cell_session=_FakeSession()
    )


def test_happy_path_order(events):
    store = _FakeStore(events=events)
    mapping = provision_space(_deps(store), "sp1")
    # 顺序：EX → Pulsar → RMS → 注册（先存储后注册）
    assert events == ["ex", "pulsar", "graph", "register"]
    assert mapping.cell_id == "cell-local"
    assert mapping.status == SpaceStatus.ACTIVE
    assert mapping.tier == Tier.COLD
    assert store.get_space_mapping("sp1") == mapping


def test_hot_tier_preopens_graph(events):
    store = _FakeStore()
    deps = _deps(store)
    provision_space(deps, "sp1", tier=Tier.HOT)
    assert any("ConfiguredGraphFactory.open" in s for s in deps.gremlin.scripts)


def test_cold_tier_no_preopen(events):
    store = _FakeStore()
    deps = _deps(store)
    provision_space(deps, "sp1")
    assert not any("ConfiguredGraphFactory.open" in s for s in deps.gremlin.scripts)


def test_graph_step_failure_rolls_back_storage(events, monkeypatch):
    def boom(client, gname, backend_props=None):
        events.append("graph")
        raise RuntimeError("gremlin down")

    monkeypatch.setattr(provision_mod, "ensure_graph_schema", boom)
    store = _FakeStore()
    deps = _deps(store)
    with pytest.raises(ProvisionError, match="已回滚"):
        provision_space(deps, "sp1")
    # 注册未执行；已完成存储逆序回滚：先删 Pulsar namespace，再 DROP EX keyspace
    assert events == ["ex", "pulsar", "graph", "rb:pulsar"]
    assert deps.ex_session.cql == ["DROP KEYSPACE IF EXISTS ex_sp1"]
    with pytest.raises(SpaceNotFoundError):
        store.get_space_mapping("sp1")


def test_register_failure_rolls_back_all(events, monkeypatch):
    monkeypatch.setattr(
        provision_mod,
        "ensure_ex_keyspace",
        lambda session, sid: events.append("ex"),
    )
    store = _FakeStore(fail_register=True, events=events)
    with pytest.raises(ProvisionError):
        provision_space(_deps(store), "sp1")
    # 注册失败后存储全部回滚
    assert "rb:graph" in events and "rb:pulsar" in events
    assert events.index("rb:graph") < events.index("rb:pulsar")  # 逆序


def test_rollback_failure_raises_rollback_error(events, monkeypatch):
    def boom(client, gname, backend_props=None):
        raise RuntimeError("gremlin down")

    monkeypatch.setattr(provision_mod, "ensure_graph_schema", boom)
    monkeypatch.setattr(
        provision_mod.pulsar_admin,
        "delete_namespace",
        lambda url, tenant, ns: (_ for _ in ()).throw(RuntimeError("pulsar admin down")),
    )
    store = _FakeStore()
    with pytest.raises(ProvisionRollbackError, match="回滚残留"):
        provision_space(_deps(store), "sp1")


def test_idempotent_when_registered(events):
    store = _FakeStore()
    store._spaces["sp1"] = SpaceMapping(
        space_id="sp1", cell_id="cell-local", ex_cluster_id="ex", pulsar_cluster_id="p"
    )
    mapping = provision_space(_deps(store), "sp1")
    assert mapping.space_id == "sp1"
    assert events == []  # 已注册 = 存储齐备（先存储后注册），零副作用


def test_invalid_space_id_rejected(events):
    with pytest.raises(ValueError):
        provision_space(_deps(_FakeStore()), "INVALID-UPPER")
    assert events == []


def test_no_open_cell_zero_side_effects(events):
    store = _FakeStore(cells=[CellInfo(cell_id="c1", watermark_state=WatermarkState.CLOSED)])
    with pytest.raises(NoOpenCellError):
        provision_space(_deps(store), "sp1")
    assert events == []
