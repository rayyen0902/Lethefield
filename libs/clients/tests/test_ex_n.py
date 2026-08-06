"""ex_n 单测：keyspace 命名 fail-closed、n_now 缓存优先与 MAX(n) 重建、EX 读访问层（M7）。"""

from datetime import UTC, datetime

import pytest
from lethefield_clients.ex_n import (
    EXPERIENCE_TABLE,
    META_TABLE,
    keyspace_name,
    list_experience_events,
    list_meta_events,
    n_key,
    n_now,
)


class FakeRedis:
    def __init__(self, data: dict[str, int] | None = None) -> None:
        self.data = dict(data or {})

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value: int) -> None:
        self.data[key] = value


class FakeRow:
    def __init__(self, mx) -> None:
        self.mx = mx


class FakeSession:
    """记录执行的 CQL，按预设返回 MAX(n) 行。"""

    def __init__(self, mx) -> None:
        self.mx = mx
        self.queries: list[str] = []

    def execute(self, query: str):
        self.queries.append(query)
        return self

    def one(self):
        return FakeRow(self.mx)


def test_keyspace_name_ok():
    assert keyspace_name("demo") == "ex_demo"
    assert keyspace_name("a1_b2") == "ex_a1_b2"


@pytest.mark.parametrize("bad", ["", "A", "has-dash", "x" * 41, "has space"])
def test_keyspace_name_fail_closed(bad):
    with pytest.raises(ValueError, match="命名约束"):
        keyspace_name(bad)


def test_n_key():
    assert n_key("demo") == "ex:n:demo"


def test_n_now_cache_hit_skips_ex():
    session = FakeSession(mx=99)
    redis = FakeRedis({"ex:n:demo": 7})
    assert n_now(redis, session, space_id="demo") == 7
    assert session.queries == []  # 缓存命中不查 EX


def test_n_now_rebuild_from_max():
    session = FakeSession(mx=42)
    redis = FakeRedis()
    assert n_now(redis, session, space_id="demo") == 42
    assert f"FROM ex_demo.{EXPERIENCE_TABLE}" in session.queries[0]
    assert redis.data["ex:n:demo"] == 42  # 重建后回写缓存


def test_n_now_empty_space_is_zero():
    session = FakeSession(mx=None)
    redis = FakeRedis()
    assert n_now(redis, session, space_id="demo") == 0
    assert redis.data["ex:n:demo"] == 0


# ---------------------------------------------------------------- EX 读访问层（M7）


class Row:
    """按属性名任意构造的 CQL 行替身。"""

    def __init__(self, **kwargs) -> None:
        self.__dict__.update(kwargs)


class ListFakeSession:
    """记录 CQL 与参数，按调用序返回预设行集。"""

    def __init__(self, result_sets: list[list]) -> None:
        self.result_sets = list(result_sets)
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, query: str, params: tuple = ()):
        self.calls.append((query, params))
        rows = self.result_sets.pop(0) if self.result_sets else []
        return type("R", (), {"all": staticmethod(lambda: rows)})()


def test_list_experience_events_sorted_by_n():
    rows = [
        Row(
            n=2,
            event_id="e2",
            content="c2",
            agent_actor_id="a",
            account_id="acc",
            tau_ms=200,
            ref_conflict="old",
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
        ),
        Row(
            n=1,
            event_id="e1",
            content="c1",
            agent_actor_id=None,
            account_id=None,
            tau_ms=None,
            ref_conflict=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        ),
    ]
    session = ListFakeSession([rows])
    events = list_experience_events(session, space_id="demo")
    assert [e.n for e in events] == [1, 2]  # 按 n 升序（重放序）
    assert events[1].ref_conflict == "old"
    assert events[0].tau_ms is None
    query, _ = session.calls[0]
    assert f"FROM ex_demo.{EXPERIENCE_TABLE}" in query
    assert "%s" not in query or session.calls[0][1] == ()  # 全表扫无参数


def test_list_meta_events_filter_by_node_key():
    rows = [
        Row(
            node_key="k1",
            created_at=datetime(2026, 1, 2, tzinfo=UTC),
            event_id="m2",
            meta_type="reinforce",
            count=3,
            n_at_event=9,
            agent_actor_id="a",
            account_id="acc",
        ),
        Row(
            node_key="k1",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            event_id="m1",
            meta_type="reinforce",
            count=1,
            n_at_event=5,
            agent_actor_id="a",
            account_id="acc",
        ),
    ]
    session = ListFakeSession([rows])
    metas = list_meta_events(session, space_id="demo", node_key="k1")
    assert [m.event_id for m in metas] == ["m1", "m2"]  # created_at 升序
    assert metas[1].count == 3
    query, params = session.calls[0]
    assert f"FROM ex_demo.{META_TABLE}" in query
    assert "WHERE node_key = %s" in query
    assert params == ("k1",)


def test_list_meta_events_full_scan_without_filter():
    session = ListFakeSession([[]])
    list_meta_events(session, space_id="demo")
    query, params = session.calls[0]
    assert "WHERE" not in query
    assert params == ()
