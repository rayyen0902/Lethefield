"""n 一致性巡检单测（M13 红线 3 配套）：check 判定矩阵 + collect 采集层 fake 注入。

覆盖：Redis 键缺失不告警（n_now 重建是设计路径）、EX MAX 缺失按 0、
一致/超前不告警、回退产 page 告警；collect 层 keyspace 不存在跳过、
单 space 失败不阻塞其他 space。
"""

from types import SimpleNamespace

from lethefield_clients.control_plane import SpaceMapping, StaticControlPlaneStore
from lethefield_clients.ex_n import n_key
from lethefield_ingest_dms.n_consistency import check_n_consistency, collect_n_consistency


class FakeRedis:
    """dict 桩：值按 str 存（同 redis-py 解码后形态）。"""

    def __init__(self, data: dict[str, str] | None = None) -> None:
        self.data = dict(data or {})

    def get(self, key: str):
        return self.data.get(key)


class TestCheckNConsistency:
    def test_redis_key_missing_no_alert(self):
        assert check_n_consistency(None, 5, space_id="s1") is None
        assert check_n_consistency(None, None, space_id="s1") is None

    def test_ex_max_none_treated_as_zero(self):
        assert check_n_consistency(0, None, space_id="s1") is None

    def test_equal_no_alert(self):
        assert check_n_consistency(5, 5, space_id="s1") is None

    def test_redis_ahead_no_alert(self):
        assert check_n_consistency(6, 5, space_id="s1") is None

    def test_regressed_page_alert(self):
        event = check_n_consistency(3, 5, space_id="s1")
        assert event is not None
        assert event.service == "ingest-dms"
        assert event.event_type == "ex_n_regressed"
        assert event.space_id == "s1"
        assert event.payload["level"] == "page"
        assert event.payload["space_id"] == "s1"
        assert event.payload["redis_n"] == 3
        assert event.payload["ex_max"] == 5
        assert "重复分配" in event.payload["message"]


class FakeExSession:
    """keyspace → MAX(n) 桩；未登记的 keyspace 抛异常（模拟 keyspace 不存在）。"""

    def __init__(self, maxes: dict[str, int | None]) -> None:
        self._maxes = maxes

    def execute(self, cql: str):
        ks = cql.split("FROM ", 1)[1].split(".", 1)[0]
        if ks not in self._maxes:
            raise RuntimeError(f"Keyspace {ks} does not exist")
        return SimpleNamespace(one=lambda: SimpleNamespace(mx=self._maxes[ks]))


def _store(*spaces: str) -> StaticControlPlaneStore:
    store = StaticControlPlaneStore.local()
    for space in spaces:
        store.register_space(
            SpaceMapping(
                space_id=space,
                cell_id="cell-local",
                ex_cluster_id="ex-local",
                pulsar_cluster_id="pulsar-local",
            )
        )
    return store


def test_collect_reports_regressed_only():
    redis = FakeRedis({n_key("ok"): "5", n_key("bad"): "2"})
    store = _store("ok", "bad")
    session = FakeExSession({"ex_ok": 5, "ex_bad": 4})
    events = collect_n_consistency(redis, session, store)
    assert [e.space_id for e in events] == ["bad"]
    assert events[0].payload["redis_n"] == 2
    assert events[0].payload["ex_max"] == 4


def test_collect_missing_keyspace_skipped_not_blocking():
    # ex_gone keyspace 不存在 → 跳过该 space，不阻塞其他 space 的判定
    redis = FakeRedis({n_key("gone"): "1", n_key("bad"): "2"})
    store = _store("gone", "bad")
    session = FakeExSession({"ex_bad": 4})
    events = collect_n_consistency(redis, session, store)
    assert [e.space_id for e in events] == ["bad"]


def test_collect_missing_redis_key_no_alert():
    redis = FakeRedis()
    store = _store("s1")
    session = FakeExSession({"ex_s1": 9})
    assert collect_n_consistency(redis, session, store) == []
