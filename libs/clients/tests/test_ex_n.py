"""ex_n 单测：keyspace 命名 fail-closed、n_now 缓存优先与 MAX(n) 重建。"""

import pytest
from lethefield_clients.ex_n import EXPERIENCE_TABLE, keyspace_name, n_key, n_now


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
