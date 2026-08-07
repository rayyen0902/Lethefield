"""训练控制 topic consumer backlog 监控（page 级，契约 5 硬性约束 2）。

consumer 停摆 = 销毁指令静默积压 = 合规失效。backlog 数值经 admin REST
topic stats 查询（本包内自实现，不 import lethefield_scheduler——服务边界）；
"持续非零"的判定时锚在 Redis 状态键（首次非零时间戳），归零即清除。

check_backlog 是纯判定函数（redis 注入，单测 dict 桩即可）；
fetch_training_backlog 的 http_get 可注入 fake。
"""

from collections.abc import Callable
from datetime import UTC, datetime

import prometheus_client
import redis as redis_lib
import requests
from lethefield_clients.training_control import (
    CONTROL_NAMESPACE,
    DESTROY_TOPIC,
    TRAINING_TENANT,
    control_topic,
)
from lethefield_logschema import LogEvent
from lethefield_metrics import gauge

SERVICE = "ingest-dms"

# 训练管线的销毁指令 sink 订阅名（契约 5 consumer；M11 加工 worker 挂此订阅）
DESTROY_SINK_SUBSCRIPTION = "training-destroy-sink"

# backlog 首次非零时间戳的 Redis 状态键（持续非零判定的时锚）
BACKLOG_SINCE_KEY = "dms:backlog_since:training_control"

# backlog 观测值指标（显式传 REGISTRY——registry=None 是 prometheus_client 原义"不注册"，
# 项目踩过的坑；space 粒度明细不走指标，namespace_class 是低基数枚举、在白名单内）
BACKLOG_GAUGE = gauge(
    "lethefield_pulsar_backlog_events",
    "训练控制 topic consumer backlog（销毁指令积压条数）",
    labels=["namespace_class"],
    registry=prometheus_client.REGISTRY,
)


def report_backlog(backlog: int) -> None:
    """把 backlog 观测值写入 gauge（每轮巡检都报，含 0）。"""
    BACKLOG_GAUGE.labels(namespace_class="training_control").set(backlog)


def fetch_training_backlog(
    admin_url: str,
    *,
    http_get: Callable[..., requests.Response] = requests.get,
) -> int:
    """取销毁控制 topic 的 sink 订阅 backlog；订阅尚不存在（无 consumer 挂过）按 0 计。"""
    stats = http_get(
        f"{admin_url}/admin/v2/persistent/"
        f"{TRAINING_TENANT}/{CONTROL_NAMESPACE}/{DESTROY_TOPIC}/stats",
        timeout=10,
    ).json()
    subscription = stats.get("subscriptions", {}).get(DESTROY_SINK_SUBSCRIPTION)
    if subscription is None:
        return 0
    return int(subscription.get("msgBacklog", 0))


def check_backlog(
    redis: redis_lib.Redis,
    backlog: int,
    *,
    now: datetime,
    stale_seconds: float,
) -> list[LogEvent]:
    """backlog 持续非零超过 stale_seconds → page 级告警；归零清除状态键。"""
    if backlog <= 0:
        redis.delete(BACKLOG_SINCE_KEY)
        return []
    raw = redis.get(BACKLOG_SINCE_KEY)
    if raw is None:
        # 首次观察到非零：只记时锚，不告警（瞬时积压是正常消费节奏）
        redis.set(BACKLOG_SINCE_KEY, now.isoformat())
        return []
    text = raw.decode() if isinstance(raw, bytes) else raw
    since = datetime.fromisoformat(text)
    if since.tzinfo is None:
        since = since.replace(tzinfo=UTC)
    stalled_for = (now - since).total_seconds()
    if stalled_for <= stale_seconds:
        return []
    return [
        LogEvent(
            service=SERVICE,
            event_type="training_control_backlog_stalled",
            payload={
                "level": "page",
                "topic": control_topic(),
                "subscription": DESTROY_SINK_SUBSCRIPTION,
                "backlog": backlog,
                "stalled_since": since.isoformat(),
                "stalled_for_seconds": stalled_for,
                "stale_seconds": stale_seconds,
            },
        )
    ]
