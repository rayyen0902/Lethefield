"""注销流水线单测：五步严格按序（红线 5：驱逐计算实例先于任何 DROP）+ 无残留校验。"""

import pytest
from lethefield_clients import (
    MappingCache,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
)
from lethefield_scheduler import destroy as destroy_mod
from lethefield_scheduler.destroy import DestroyDeps, DestroyError, destroy_space


class _FakeStore:
    def __init__(self, events: list[str]) -> None:
        self._spaces = {
            "sp1": SpaceMapping(
                space_id="sp1", cell_id="cell-local", ex_cluster_id="ex", pulsar_cluster_id="p"
            )
        }
        self._events = events

    def get_space_mapping(self, space_id):
        try:
            return self._spaces[space_id]
        except KeyError:
            raise SpaceNotFoundError(space_id) from None

    def update_space_status(self, space_id, status):
        self._events.append(f"status:{status}")
        m = self._spaces[space_id]
        self._spaces[space_id] = SpaceMapping(
            space_id=m.space_id,
            cell_id=m.cell_id,
            ex_cluster_id=m.ex_cluster_id,
            pulsar_cluster_id=m.pulsar_cluster_id,
            status=status,
        )

    def unregister_space(self, space_id):
        self._events.append("unregister")
        del self._spaces[space_id]


class _FakeResult:
    def __init__(self, value) -> None:
        self._value = value

    def all(self):
        return self

    def result(self):
        return self._value


class _FakeGremlin:
    def __init__(self, events: list[str], graph_names=None) -> None:
        self._events = events
        self._graph_names = graph_names if graph_names is not None else []

    def submit(self, script, bindings=None):
        if script.strip() == "ConfiguredGraphFactory.getGraphNames()":
            return _FakeResult(self._graph_names)
        assert "ConfiguredGraphFactory.close" in script  # 驱逐必须经 close
        self._events.append("evict")
        return _FakeResult(["evicted"])


class _FakeSession:
    def __init__(self, events: list[str], tag: str, residue: bool = False) -> None:
        self._events = events
        self._tag = tag
        self._residue = residue

    def execute(self, statement, parameters=None, **kwargs):
        if statement.startswith("DROP"):
            self._events.append(f"drop:{self._tag}")
            return None

        class _Rs:
            def one(self_inner):
                return object() if self._residue else None

        return _Rs()


class _FakeEs:
    def __init__(self, events: list[str], count: int = 0) -> None:
        self._events = events
        self._count = count

    def options(self, ignore_status=()):
        return self

    def delete_by_query(self, **kwargs):
        self._events.append("es:delete")

    def count(self, **kwargs):
        return type("Resp", (), {"body": {"count": self._count}})()


def _deps(events, *, es_count=0, graph_names=None, cell_residue=False) -> DestroyDeps:
    return DestroyDeps(
        store=_FakeStore(events),
        gremlin=_FakeGremlin(events, graph_names=graph_names),
        cell_session=_FakeSession(events, "graph_ks", residue=cell_residue),
        ex_session=_FakeSession(events, "ex_ks"),
        es=_FakeEs(events, count=es_count),
    )


def test_destroy_order_close_before_drop(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(
        destroy_mod.pulsar_admin,
        "delete_namespace",
        lambda url, tenant, ns: events.append("pulsar:delete"),
    )
    broadcast: list[str] = []
    destroy_space(_deps(events), "sp1", broadcast_destroy=broadcast.append)
    assert events == [
        f"status:{SpaceStatus.DESTROYING}",
        "evict",  # 红线 5：驱逐计算实例必须先于任何 DROP
        "es:delete",  # 先派生物（ES 文档 + RMS 图）
        "drop:graph_ks",
        "pulsar:delete",
        "drop:ex_ks",  # 本体最后
        "unregister",
    ]
    assert broadcast == ["sp1"]  # 广播实际调用（不是留空）


def test_destroy_invalidates_cache(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(
        destroy_mod.pulsar_admin, "delete_namespace", lambda *a: events.append("pulsar:delete")
    )
    deps = _deps(events)
    cache = MappingCache(deps.store, ttl_seconds=1000)
    cache.get_space_mapping("sp1")  # 预热
    destroy_space(deps, "sp1", cache=cache)
    with pytest.raises(SpaceNotFoundError):
        cache.get_space_mapping("sp1")  # 缓存已失效，直达 store 抛 404


def test_destroy_unregistered_space_fails_closed(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(destroy_mod.pulsar_admin, "delete_namespace", lambda *a: None)
    with pytest.raises(SpaceNotFoundError):
        destroy_space(_deps(events), "ghost")
    assert events == []  # 零副作用


def test_destroy_residue_raises(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(
        destroy_mod.pulsar_admin, "delete_namespace", lambda *a: events.append("pulsar:delete")
    )
    deps = _deps(events, cell_residue=True)  # 图 keyspace DROP 未生效
    with pytest.raises(DestroyError, match="残留"):
        destroy_space(deps, "sp1")


def test_destroy_es_residue_raises(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(destroy_mod.pulsar_admin, "delete_namespace", lambda *a: None)
    deps = _deps(events, es_count=3)
    with pytest.raises(DestroyError, match="rms_vectors"):
        destroy_space(deps, "sp1")


def test_destroy_graph_config_residue_raises(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(destroy_mod.pulsar_admin, "delete_namespace", lambda *a: None)
    deps = _deps(events, graph_names=["sp1"])  # removeConfiguration 未生效
    with pytest.raises(DestroyError, match="ConfiguredGraphFactory"):
        destroy_space(deps, "sp1")


def test_default_broadcast_is_logging_not_noop(monkeypatch, caplog):
    events: list[str] = []
    monkeypatch.setattr(destroy_mod.pulsar_admin, "delete_namespace", lambda *a: None)
    import logging

    with caplog.at_level(logging.INFO, logger="lethefield_scheduler.destroy"):
        destroy_space(_deps(events), "sp1")  # 不传 broadcast_destroy
    assert any("sp1" in r.message for r in caplog.records)
