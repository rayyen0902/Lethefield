"""M10 EX 摄入 Dead Man's Switch 集成测试（真实 Pulsar/Redis/控制面，默认栈）。

覆盖 M10 验收第 5 条：模拟某 space 停止写入超过设定窗口 → 主动探测触发告警
（不依赖"系统看起来在正常运行"）。三路：
1. 管道活性探针（监控 tenant probe topic 真实收发 ack）；
2. 训练控制 topic consumer backlog 停滞（真实积压 → page 告警 → 排空恢复）；
3. space 写入新鲜度（真实 Redis 时标 + 控制面枚举，翻转边告警/恢复）。
"""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from lethefield_clients import (
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    MappingTableControlPlaneStore,
    SpaceMapping,
    Tier,
    cassandra_cluster,
    local_cell,
    pulsar_client,
    redis_client,
    touch_last_write,
)
from lethefield_clients.ex_n import last_write_key
from lethefield_ingest_dms.backlog import (
    BACKLOG_SINCE_KEY,
    check_backlog,
    fetch_training_backlog,
)
from lethefield_ingest_dms.config import DmsConfig
from lethefield_ingest_dms.freshness import STALE_KEY_PREFIX, check_freshness
from lethefield_ingest_dms.probe import (
    ensure_monitoring_topic,
    probe_pipeline,
    pulsar_probe_roundtrip,
)
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.destroy_broadcast import make_broadcast
from lethefield_scheduler.training_control_sink import run_once


@pytest.fixture(scope="module")
def stack():
    config = DmsConfig.from_env()
    ensure_monitoring_topic(config.pulsar_admin_url)
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE)
    cell_cluster = cassandra_cluster()
    store = MappingTableControlPlaneStore(cell_cluster.connect())
    store.ensure_tables()
    yield SimpleNamespace(
        config=config,
        store=store,
        redis=redis_client(),
        pulsar_admin_url=config.pulsar_admin_url,
    )
    cell_cluster.shutdown()


def test_probe_roundtrip_healthy(stack):
    """管道活性探针：真实发→收→ack 往返，健康栈上零告警。"""
    alerts = probe_pipeline(lambda: pulsar_probe_roundtrip(stack.config))
    assert alerts == []


def test_backlog_stall_triggers_page_alert(stack):
    """consumer 停摆（sink 不消费）→ 销毁指令积压超窗 → page 级告警；排空后恢复。"""
    redis = stack.redis
    redis.delete(BACKLOG_SINCE_KEY)
    sink = pulsar_client()
    try:
        run_once(sink, emit=lambda e: None, timeout_ms=500)  # 建 durable 订阅后停消费（停摆）
        producer_client = pulsar_client()
        try:
            make_broadcast(producer_client)(f"m10_dms_{uuid.uuid4().hex[:6]}")  # 积压一条真实指令
        finally:
            producer_client.close()

        backlog = fetch_training_backlog(stack.pulsar_admin_url)
        assert backlog >= 1  # 真实积压（consumer 停摆）

        t0 = datetime.now(UTC)
        assert check_backlog(redis, backlog, now=t0, stale_seconds=60) == []  # 首次非零只记时锚
        alerts = check_backlog(redis, backlog, now=t0 + timedelta(seconds=61), stale_seconds=60)
        assert len(alerts) == 1
        assert alerts[0].event_type == "training_control_backlog_stalled"
        assert alerts[0].payload["level"] == "page"

        assert run_once(sink, emit=lambda e: None, timeout_ms=10_000) >= 1  # 排空
        assert fetch_training_backlog(stack.pulsar_admin_url) == 0
        assert check_backlog(redis, 0, now=datetime.now(UTC), stale_seconds=60) == []
        assert redis.get(BACKLOG_SINCE_KEY) is None  # 状态键已清
    finally:
        sink.close()
        redis.delete(BACKLOG_SINCE_KEY)


def test_space_write_stale_and_recover(stack):
    """模拟活跃 space 停止写入超窗 → observation 级告警（翻转边一次）；恢复写入 → recovered。"""
    redis = stack.redis
    space_id = f"m10_dms_{uuid.uuid4().hex[:6]}"
    stack.store.register_space(
        SpaceMapping(
            space_id=space_id,
            cell_id=local_cell().cell_id,
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
            tier=Tier.COLD,
        )
    )
    now = datetime.now(UTC)
    try:
        touch_last_write(redis, space_id, now=now)  # 刚写入：活跃且新鲜
        assert (
            check_freshness(
                redis,
                stack.store,
                now=now,
                activity_window_seconds=3600,
                stale_threshold_seconds=600,
            )
            == []
        )

        # 停止写入超窗（last_write 停在 11 分钟前，仍在活跃窗口内）
        stale_since = now - timedelta(seconds=660)
        redis.set(last_write_key(space_id), stale_since.isoformat())
        alerts = check_freshness(
            redis, stack.store, now=now, activity_window_seconds=3600, stale_threshold_seconds=600
        )
        assert len(alerts) == 1
        assert alerts[0].event_type == "space_write_stale"
        assert alerts[0].space_id == space_id
        assert alerts[0].payload["level"] == "observation"

        # 持续 stale 不重复刷（翻转边语义）
        assert (
            check_freshness(
                redis,
                stack.store,
                now=now + timedelta(seconds=60),
                activity_window_seconds=3600,
                stale_threshold_seconds=600,
            )
            == []
        )

        # 恢复写入 → recovered 事件 + 状态键清除
        touch_last_write(redis, space_id, now=now + timedelta(seconds=120))
        alerts = check_freshness(
            redis,
            stack.store,
            now=now + timedelta(seconds=120),
            activity_window_seconds=3600,
            stale_threshold_seconds=600,
        )
        assert [a.event_type for a in alerts] == ["space_write_recovered"]
        assert redis.get(STALE_KEY_PREFIX + space_id) is None
    finally:
        stack.store.unregister_space(space_id)
        redis.delete(last_write_key(space_id), STALE_KEY_PREFIX + space_id)


def test_hot_tier_never_written_is_stale(stack):
    """hot/premium 全集纳入监控：从未写入即 stale（高保障 tier 的沉默本身是异常）。"""
    redis = stack.redis
    space_id = f"m10_dms_{uuid.uuid4().hex[:6]}"
    stack.store.register_space(
        SpaceMapping(
            space_id=space_id,
            cell_id=local_cell().cell_id,
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
            tier=Tier.HOT,
        )
    )
    try:
        alerts = check_freshness(
            redis,
            stack.store,
            now=datetime.now(UTC),
            activity_window_seconds=3600,
            stale_threshold_seconds=600,
        )
        assert [a.event_type for a in alerts] == ["space_write_stale"]
        assert alerts[0].payload["last_write_at"] is None
    finally:
        stack.store.unregister_space(space_id)
        redis.delete(STALE_KEY_PREFIX + space_id)
