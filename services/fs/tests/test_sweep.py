"""FS sweep 单测（不需要栈）：纯判定矩阵、快照构建、liveness 判定、worker 指标映射。

图/存储交互路径由集成测试覆盖（tests/integration/test_m6_fs.py）。
"""

from datetime import UTC, datetime

from lethefield_fs.archive import build_snapshot
from lethefield_fs.config import DEFAULT_SWEEP_CONFIG, HEARTBEAT_KEY
from lethefield_fs.liveness import check_liveness
from lethefield_fs.sweep import consolidate_due, neglect_due, refresh_due

N_NEGLECT = 20


class TestNeglectDue:
    """忽视区间判定：n_now − n_last_touched ≥ (neglect_count+1)×N_neglect。"""

    def test_first_interval_not_yet(self):
        assert not neglect_due(n_now=19, n_last_touched=0, neglect_count=0, n_neglect=N_NEGLECT)

    def test_first_interval_reached(self):
        assert neglect_due(n_now=20, n_last_touched=0, neglect_count=0, n_neglect=N_NEGLECT)

    def test_same_interval_no_repeat(self):
        # 已惩罚一次（neglect_count=1）后，同一区间内重复判定不触发——幂等的来源
        assert not neglect_due(n_now=20, n_last_touched=0, neglect_count=1, n_neglect=N_NEGLECT)
        assert not neglect_due(n_now=39, n_last_touched=0, neglect_count=1, n_neglect=N_NEGLECT)

    def test_next_interval_triggers(self):
        assert neglect_due(n_now=40, n_last_touched=0, neglect_count=1, n_neglect=N_NEGLECT)

    def test_far_behind_triggers_once_per_sweep(self):
        # sweep 停摆很久后恢复：一轮只补一记惩罚（计数器步进，下一区间等下一轮）
        assert neglect_due(n_now=1000, n_last_touched=0, neglect_count=2, n_neglect=N_NEGLECT)


class TestConsolidateDue:
    def test_threshold_reached_no_conflict(self):
        assert consolidate_due(reinforce_count=3, conflict_count=0, threshold=3)

    def test_below_threshold(self):
        assert not consolidate_due(reinforce_count=2, conflict_count=0, threshold=3)

    def test_conflict_blocks(self):
        assert not consolidate_due(reinforce_count=5, conflict_count=1, threshold=3)


class TestRefreshDue:
    def test_within_margin(self):
        assert refresh_due(n_now=90, n_star_cached=100, margin=20)

    def test_beyond_margin(self):
        assert not refresh_due(n_now=50, n_star_cached=100, margin=20)

    def test_already_crossed_not_refreshed(self):
        # 已跨界节点走归档判定，不做顺带刷新
        assert not refresh_due(n_now=120, n_star_cached=100, margin=20)


class TestBuildSnapshot:
    def test_tau_serialized_to_epoch_ms(self):
        snapshot = build_snapshot(
            {"content": "c", "tau": datetime(2026, 8, 5, tzinfo=UTC), "s": 0.1},
            [{"label": "temporal", "out_key": "a", "in_key": "b"}],
        )
        assert snapshot["props"]["tau"] == 1785888000000
        assert snapshot["props"]["s"] == 0.1
        assert snapshot["edges"][0]["label"] == "temporal"

    def test_naive_datetime_interpreted_as_utc(self):
        # gremlin_python 反序列化的 Date 是 naive datetime（UTC 语义）
        snapshot = build_snapshot({"tau": datetime(2026, 8, 5)}, [])
        assert snapshot["props"]["tau"] == 1785888000000


class FakeRedis:
    def __init__(self, data: dict[str, float] | None = None) -> None:
        self.data = dict(data or {})

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value: float) -> None:
        self.data[key] = value


class TestLiveness:
    def test_no_heartbeat_alerts(self):
        alerts = check_liveness(FakeRedis(), stale_after_seconds=300, now=1000.0)
        assert len(alerts) == 1
        assert "无心跳" in alerts[0]

    def test_fresh_heartbeat_ok(self):
        assert (
            check_liveness(FakeRedis({HEARTBEAT_KEY: 900.0}), stale_after_seconds=300, now=1000.0)
            == []
        )

    def test_stale_heartbeat_alerts(self):
        alerts = check_liveness(
            FakeRedis({HEARTBEAT_KEY: 600.0}), stale_after_seconds=300, now=1000.0
        )
        assert len(alerts) == 1
        assert "心跳停滞" in alerts[0]

    def test_boundary_is_fresh(self):
        assert (
            check_liveness(FakeRedis({HEARTBEAT_KEY: 700.0}), stale_after_seconds=300, now=1000.0)
            == []
        )


def test_default_config_placeholders():
    config = DEFAULT_SWEEP_CONFIG
    assert config.consolidate_reinforce_threshold == 3
    assert config.near_horizon_margin == 20
    assert config.stale_after_seconds > config.sweep_interval_seconds
