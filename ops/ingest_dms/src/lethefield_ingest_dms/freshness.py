"""space 写入新鲜度监控（observation 级）。

数据源：`lethefield_clients.ex_n.last_write_at`（摄入路径在每次成功写入后维护，
DMS 的"活跃"语义锚点是成功摄入，不是"看起来在运行"）；space 枚举经
ControlPlaneStore 抽象（list_spaces + get_space_mapping 取 tier），禁直连业务存储。

翻转边留痕：Redis 状态键 `dms:stale:{space_id}`——fresh→stale 翻转告警一次，
持续 stale 不重复刷，stale→fresh 翻转清键并产 recovered 事件。

check_freshness 是纯判定函数（redis/store 注入，单测 dict 桩 + StaticControlPlaneStore）。
"""

from datetime import datetime

import redis as redis_lib
from lethefield_clients.control_plane import ControlPlaneStore, Tier
from lethefield_clients.ex_n import last_write_at
from lethefield_logschema import LogEvent
from lethefield_metrics import gauge as _metric_gauge
from prometheus_client import REGISTRY as _DEFAULT_REGISTRY

SERVICE = "ingest-dms"

# M12 定案：ex_last_write_age_seconds（原 n_now_lag_seconds 的修正口径）——
# per-space 龄期聚合成 max/p95 两个 gauge（space_id 是标签黑名单，聚合守基数纪律）。
# 与 DMS 新鲜度告警同源同事、各走各线：告警线管呼叫，指标线管趋势。
_EX_LAST_WRITE_AGE = _metric_gauge(
    "lethefield_ex_last_write_age_seconds",
    "space 距最近一次成功 EX 写入的龄期聚合（dimension=max/p95）",
    labels=["dimension"],
    registry=_DEFAULT_REGISTRY,
)


def _p95(values: list[float]) -> float:
    """简易 p95（单调上报口径，样本少时不外推）。"""
    ordered = sorted(values)
    return ordered[max(0, int(len(ordered) * 0.95 + 0.999) - 1)]


# 高保障 tier：从未写入也算活跃且立即 stale（-hot/premium 的沉默本身就是异常）
HOT_TIERS = frozenset({Tier.HOT, Tier.PREMIUM})

# stale 状态键前缀（翻转边判定的状态载体）
STALE_KEY_PREFIX = "dms:stale:"


def check_freshness(
    redis: redis_lib.Redis,
    store: ControlPlaneStore,
    *,
    now: datetime,
    activity_window_seconds: float,
    stale_threshold_seconds: float,
) -> list[LogEvent]:
    """活跃集合内 stale 判定 + 翻转边告警。返回本轮产生的告警/恢复事件。"""
    alerts: list[LogEvent] = []
    ages: list[float] = []
    for space_id in store.list_spaces():
        mapping = store.get_space_mapping(space_id)
        last_write = last_write_at(redis, space_id)
        age_seconds = (now - last_write).total_seconds() if last_write is not None else None
        if age_seconds is not None:
            ages.append(age_seconds)
        state_key = STALE_KEY_PREFIX + space_id

        # 活跃集合：W 窗口内有写入的 space，或 hot/premium tier 全集；
        # 冷 space 长期（超 W）无写入不纳入监控——低活跃不是故障
        in_window = age_seconds is not None and age_seconds <= activity_window_seconds
        if not in_window and mapping.tier not in HOT_TIERS:
            # 离开监控视野：清残留状态键（重入活跃集合时按新翻转计）
            redis.delete(state_key)
            continue

        stale = last_write is None or age_seconds > stale_threshold_seconds
        was_stale = redis.get(state_key) is not None

        if stale and not was_stale:
            redis.set(state_key, now.isoformat())
            alerts.append(
                LogEvent(
                    service=SERVICE,
                    event_type="space_write_stale",
                    space_id=space_id,
                    payload={
                        "level": "observation",
                        "tier": mapping.tier.value,
                        "last_write_at": (
                            last_write.isoformat() if last_write is not None else None
                        ),
                        "age_seconds": age_seconds,
                        "stale_threshold_seconds": stale_threshold_seconds,
                    },
                )
            )
        elif not stale and was_stale:
            redis.delete(state_key)
            alerts.append(
                LogEvent(
                    service=SERVICE,
                    event_type="space_write_recovered",
                    space_id=space_id,
                    payload={
                        "level": "observation",
                        "tier": mapping.tier.value,
                        "last_write_at": last_write.isoformat(),
                        "age_seconds": age_seconds,
                    },
                )
            )
    if ages:  # M12：龄期聚合 gauge（无可算龄期的 space 时保持上轮值）
        _EX_LAST_WRITE_AGE.labels(dimension="max").set(max(ages))
        _EX_LAST_WRITE_AGE.labels(dimension="p95").set(_p95(ages))
    return alerts
