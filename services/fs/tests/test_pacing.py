"""FS sweep 冷热分频 run_once 对比测试（M13 红线 3 验收「对比测试」）。

全 fake 注入（FakeRedis / 无 session 的映射表桩 / monkeypatch sweep_space 与 n_now），
验证：冷 space 心跳新鲜时跳过、过期后执行、hot space 始终按热节奏执行、
tier 映射取不到时全部按 hot（保障优先）、心跳解析失败 fail-open 向扫。
"""

import time

import pytest
from lethefield_clients.control_plane import (
    MappingTableControlPlaneStore,
    SpaceMapping,
    StaticControlPlaneStore,
    Tier,
)
from lethefield_fs import worker
from lethefield_fs.config import HEARTBEAT_KEY, SweepConfig
from lethefield_fs.sweep import SweepStats

NOW = 10_000.0
_CONFIG = SweepConfig(sweep_interval_seconds=60.0, cold_interval_seconds=600.0)


class FakeRedis:
    def __init__(self, data: dict | None = None) -> None:
        self.data = dict(data or {})

    def get(self, key: str):
        return self.data.get(key)

    def set(self, key: str, value) -> None:
        self.data[key] = value


class FakeMappingStore(MappingTableControlPlaneStore):
    """无 session 的映射表桩：只实现 run_once 用到的 list_spaces / list_space_mappings。"""

    def __init__(self, tiers: dict[str, Tier]) -> None:
        self._tiers = tiers

    def list_spaces(self) -> list[str]:
        return sorted(self._tiers)

    def list_space_mappings(self) -> list[SpaceMapping]:
        return [
            SpaceMapping(
                space_id=space,
                cell_id="cell-local",
                ex_cluster_id="ex-local",
                pulsar_cluster_id="pulsar-local",
                tier=tier,
            )
            for space, tier in sorted(self._tiers.items())
        ]


@pytest.fixture
def swept(monkeypatch):
    """替换 sweep_space / n_now 为记录桩，冻结 time.time()，返回本轮被扫的 space 列表。"""
    calls: list[str] = []
    monkeypatch.setattr(worker, "n_now", lambda redis, session, *, space_id: 100)
    monkeypatch.setattr(time, "time", lambda: NOW)

    def fake_sweep(client, cell_session, es, *, gname, space_id, n_now, config, ff_config):
        calls.append(space_id)
        return SweepStats()

    monkeypatch.setattr(worker, "sweep_space", fake_sweep)
    return calls


def run(store, redis):
    return worker.run_once(store, None, None, None, None, redis, config=_CONFIG)


def test_cold_fresh_skipped_hot_runs(swept):
    store = FakeMappingStore({"hot1": Tier.HOT, "cold1": Tier.COLD})
    redis = FakeRedis(
        {
            f"{HEARTBEAT_KEY}:hot1": NOW - 100,  # 超热阈值 60s → due
            f"{HEARTBEAT_KEY}:cold1": NOW - 100,  # 100s < 冷阈值 600s → 跳过
        }
    )
    results = run(store, redis)
    assert swept == ["hot1"]
    assert list(results) == ["hot1"]
    # 跳过的 space 不写心跳（保留原值），hot 心跳照常刷新
    assert redis.data[f"{HEARTBEAT_KEY}:cold1"] == NOW - 100
    assert redis.data[f"{HEARTBEAT_KEY}:hot1"] == NOW
    # 全局心跳仍每轮写（liveness 停摆检测语义不变）
    assert redis.data[HEARTBEAT_KEY] == NOW


def test_cold_stale_runs(swept):
    store = FakeMappingStore({"cold1": Tier.COLD})
    redis = FakeRedis({f"{HEARTBEAT_KEY}:cold1": NOW - 601})
    results = run(store, redis)
    assert swept == ["cold1"]
    assert list(results) == ["cold1"]
    assert redis.data[f"{HEARTBEAT_KEY}:cold1"] == NOW


def test_first_round_all_due(swept):
    # --once 语义：首轮无 per-space 心跳 → 全 due
    store = FakeMappingStore({"hot1": Tier.HOT, "cold1": Tier.COLD})
    results = run(store, FakeRedis())
    assert swept == ["cold1", "hot1"]
    assert list(results) == ["cold1", "hot1"]


def test_unparsable_heartbeat_fails_open(swept):
    # 心跳值解析失败按 None（恒 due，fail-open 向扫）
    store = FakeMappingStore({"cold1": Tier.COLD})
    redis = FakeRedis({f"{HEARTBEAT_KEY}:cold1": "not-a-float"})
    assert swept == []  # fixture 尚未触发
    run(store, redis)
    assert swept == ["cold1"]


def test_non_mapping_store_all_hot(swept):
    # 非映射表实现取不到 tier → 全部按 hot（保障优先）
    store = StaticControlPlaneStore.local()
    for space in ("s1", "s2"):
        store.register_space(
            SpaceMapping(
                space_id=space,
                cell_id="cell-local",
                ex_cluster_id="ex-local",
                pulsar_cluster_id="pulsar-local",
                tier=Tier.COLD,  # tier 存在但实现不透出，仍按 hot
            )
        )
    redis = FakeRedis({f"{HEARTBEAT_KEY}:s1": NOW - 100, f"{HEARTBEAT_KEY}:s2": NOW - 100})
    run(store, redis)
    assert swept == ["s1", "s2"]
