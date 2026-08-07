"""ingest_dms 单元测试：三路判定逻辑全部走注入 fake（dict 桩 redis /
StaticControlPlaneStore / fake http），不需要真栈。

覆盖：probe 成功无告警 / 异常产 page 告警；backlog 首次非零不告警、超窗告警、
归零清除；freshness 活跃集合判定、stale 翻转边只告警一次、recovered 翻转、
hot/premium 无写入即 stale、cold 超 W 不告警。
"""

from datetime import UTC, datetime, timedelta

import pytest
from lethefield_clients.control_plane import (
    SpaceMapping,
    StaticControlPlaneStore,
    Tier,
)
from lethefield_clients.ex_n import touch_last_write
from lethefield_ingest_dms.backlog import (
    BACKLOG_SINCE_KEY,
    check_backlog,
    fetch_training_backlog,
)
from lethefield_ingest_dms.freshness import STALE_KEY_PREFIX, check_freshness
from lethefield_ingest_dms.probe import ensure_monitoring_topic, probe_pipeline

NOW = datetime(2026, 8, 7, tzinfo=UTC)
W = 7 * 24 * 3600
STALE = 24 * 3600


class FakeRedis:
    """dict 桩：只实现本包用到的 get/set/delete（值按 str 存，同 redis-py 解码后形态）。"""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value: str) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        self.data.pop(key, None)


class FakeResponse:
    def __init__(self, status_code: int = 200, json_data=None, text: str = "") -> None:
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        return self._json


def make_store(entries: list[tuple[str, Tier]]) -> StaticControlPlaneStore:
    store = StaticControlPlaneStore.local()
    for space_id, tier in entries:
        store.register_space(
            SpaceMapping(
                space_id=space_id,
                cell_id="cell-local",
                ex_cluster_id="ex-local",
                pulsar_cluster_id="pulsar-local",
                tier=tier,
            )
        )
    return store


def freshness(redis, store, now=NOW, window=W, stale=STALE):
    return check_freshness(
        redis,
        store,
        now=now,
        activity_window_seconds=window,
        stale_threshold_seconds=stale,
    )


# ---------------------------------------------------------------- probe


def test_probe_success_no_alert():
    assert probe_pipeline(lambda: None) == []


def test_probe_timeout_page_alert():
    def boom():
        raise TimeoutError("receive timed out")

    alerts = probe_pipeline(boom)
    assert len(alerts) == 1
    assert alerts[0].service == "ingest-dms"
    assert alerts[0].event_type == "ingest_probe_failed"
    assert alerts[0].payload["level"] == "page"
    assert "receive timed out" in alerts[0].payload["error"]


def test_ensure_monitoring_topic_idempotent():
    calls = []

    def http_get(url, **kwargs):
        return FakeResponse(json_data=["standalone"])

    def http_put(url, **kwargs):
        calls.append(url)
        return FakeResponse(status_code=409)  # 已存在也视为成功

    ensure_monitoring_topic("http://pulsar:8080", http_get=http_get, http_put=http_put)
    assert calls == [
        "http://pulsar:8080/admin/v2/tenants/lethefield-monitoring",
        "http://pulsar:8080/admin/v2/namespaces/lethefield-monitoring/ops",
    ]


def test_ensure_monitoring_topic_failure_raises():
    def http_get(url, **kwargs):
        return FakeResponse(json_data=["standalone"])

    def http_put(url, **kwargs):
        return FakeResponse(status_code=500, text="boom")

    with pytest.raises(RuntimeError, match="建监控 tenant 失败"):
        ensure_monitoring_topic("http://pulsar:8080", http_get=http_get, http_put=http_put)


# ---------------------------------------------------------------- backlog


def test_fetch_training_backlog_parses_subscription_stats():
    stats = {"subscriptions": {"training-destroy-sink": {"msgBacklog": 7}}}

    def http_get(url, **kwargs):
        assert url.endswith("/admin/v2/persistent/lethefield-training/control/space-destroy/stats")
        return FakeResponse(json_data=stats)

    assert fetch_training_backlog("http://pulsar:8080", http_get=http_get) == 7


def test_fetch_training_backlog_missing_subscription_is_zero():
    def http_get(url, **kwargs):
        return FakeResponse(json_data={"subscriptions": {}})

    assert fetch_training_backlog("http://pulsar:8080", http_get=http_get) == 0


def test_backlog_first_nonzero_records_anchor_no_alert():
    redis = FakeRedis()
    assert check_backlog(redis, 3, now=NOW, stale_seconds=300) == []
    assert BACKLOG_SINCE_KEY in redis.data


def test_backlog_within_window_no_alert():
    redis = FakeRedis()
    check_backlog(redis, 3, now=NOW, stale_seconds=300)
    assert check_backlog(redis, 5, now=NOW + timedelta(seconds=299), stale_seconds=300) == []


def test_backlog_stalled_over_window_page_alert():
    redis = FakeRedis()
    check_backlog(redis, 3, now=NOW, stale_seconds=300)
    alerts = check_backlog(redis, 5, now=NOW + timedelta(seconds=301), stale_seconds=300)
    assert len(alerts) == 1
    assert alerts[0].event_type == "training_control_backlog_stalled"
    assert alerts[0].payload["level"] == "page"
    assert alerts[0].payload["backlog"] == 5


def test_backlog_zero_clears_anchor():
    redis = FakeRedis()
    check_backlog(redis, 3, now=NOW, stale_seconds=300)
    assert check_backlog(redis, 0, now=NOW + timedelta(seconds=10), stale_seconds=300) == []
    assert BACKLOG_SINCE_KEY not in redis.data
    # 再次非零重新计锚，不误报
    assert check_backlog(redis, 1, now=NOW + timedelta(seconds=400), stale_seconds=300) == []


# ---------------------------------------------------------------- freshness


def test_fresh_cold_space_no_alert():
    redis = FakeRedis()
    touch_last_write(redis, "s1", now=NOW - timedelta(hours=1))
    store = make_store([("s1", Tier.COLD)])
    assert freshness(redis, store) == []


def test_stale_flip_alerts_once_then_quiet():
    redis = FakeRedis()
    touch_last_write(redis, "s1", now=NOW - timedelta(days=2))  # >1d stale，<7d 窗口内
    store = make_store([("s1", Tier.COLD)])

    alerts = freshness(redis, store)
    assert len(alerts) == 1
    assert alerts[0].event_type == "space_write_stale"
    assert alerts[0].space_id == "s1"
    assert alerts[0].payload["level"] == "observation"
    assert f"{STALE_KEY_PREFIX}s1" in redis.data

    # 持续 stale 不重复刷
    assert freshness(redis, store, now=NOW + timedelta(hours=1)) == []


def test_recovered_flip_clears_state_and_alerts():
    redis = FakeRedis()
    store = make_store([("s1", Tier.COLD)])
    touch_last_write(redis, "s1", now=NOW - timedelta(days=2))
    freshness(redis, store)  # 进入 stale

    later = NOW + timedelta(days=2, hours=1)
    touch_last_write(redis, "s1", now=later)  # 恢复 fresh
    alerts = freshness(redis, store, now=later)
    assert len(alerts) == 1
    assert alerts[0].event_type == "space_write_recovered"
    assert alerts[0].space_id == "s1"
    assert f"{STALE_KEY_PREFIX}s1" not in redis.data


def test_hot_tier_never_written_is_immediately_stale():
    redis = FakeRedis()
    store = make_store([("vip", Tier.HOT)])
    alerts = freshness(redis, store)
    assert len(alerts) == 1
    assert alerts[0].event_type == "space_write_stale"
    assert alerts[0].payload["last_write_at"] is None


def test_premium_tier_never_written_is_immediately_stale():
    redis = FakeRedis()
    store = make_store([("vip", Tier.PREMIUM)])
    assert [a.event_type for a in freshness(redis, store)] == ["space_write_stale"]


def test_cold_space_beyond_window_not_monitored():
    redis = FakeRedis()
    touch_last_write(redis, "s1", now=NOW - timedelta(days=30))  # 超 W，离开活跃集合
    store = make_store([("s1", Tier.COLD)])
    assert freshness(redis, store) == []
    # 即使曾经 stale 留过状态键，离开监控视野时也清掉
    assert f"{STALE_KEY_PREFIX}s1" not in redis.data


def test_cold_space_never_written_not_monitored():
    redis = FakeRedis()
    store = make_store([("s1", Tier.COLD)])
    assert freshness(redis, store) == []


def test_stale_state_cleared_when_leaving_window_allows_fresh_flip():
    redis = FakeRedis()
    store = make_store([("s1", Tier.COLD)])
    touch_last_write(redis, "s1", now=NOW - timedelta(days=2))
    freshness(redis, store)  # stale，留键
    # 滑出 W 窗口：状态键清除
    out = NOW + timedelta(days=6)
    assert freshness(redis, store, now=out) == []
    assert f"{STALE_KEY_PREFIX}s1" not in redis.data
