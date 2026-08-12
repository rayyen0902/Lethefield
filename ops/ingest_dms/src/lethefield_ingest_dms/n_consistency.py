"""n 一致性巡检（M13 红线 3 配套，page 级）：Redis `ex:n:{space}` vs EX `MAX(n)`。

n 是 space 级单调事件序号（契约 1：Redis INCR 分配，只有经验事件推进）。
Redis 值 < EX MAX(n) = n 回退——INCR 会重新发出已用过的序号（重复分配风险），
必须 page。Redis 键缺失不告警：n_now 从 EX MAX(n) 重建是设计路径
（lethefield_clients.ex_n.n_now），重建后的值 ≥ EX MAX，天然一致。
"""

import redis as redis_lib
from cassandra.cluster import Session
from lethefield_clients.control_plane import ControlPlaneStore
from lethefield_clients.ex_n import EXPERIENCE_TABLE, keyspace_name, n_key
from lethefield_clients.redline import redline1_exempt
from lethefield_logschema import LogEvent

SERVICE = "ingest-dms"  # 与 __main__.SERVICE 一致


def check_n_consistency(
    redis_n: int | None, ex_max: int | None, *, space_id: str
) -> LogEvent | None:
    """纯判定：Redis n 与 EX MAX(n) 比对。仅回退告警，其余（含键缺失）放行。"""
    if redis_n is None:
        return None  # 键缺失：n_now 重建是设计路径，不告警
    ex_max = ex_max if ex_max is not None else 0
    if redis_n < ex_max:
        return LogEvent(
            service=SERVICE,
            event_type="ex_n_regressed",
            space_id=space_id,
            payload={
                "level": "page",
                "space_id": space_id,
                "redis_n": redis_n,
                "ex_max": ex_max,
                "message": "n 回退：Redis ex:n 小于 EX MAX(n)，序号重复分配风险",
            },
        )
    return None


@redline1_exempt(
    worker="ingest-dms/n-consistency",
    reason=(
        "枚举走 ControlPlaneStore.list_spaces()（映射表 active 集合）；"
        "逐 space 独立比对 Redis ex:n 与该 space EX MAX(n)；批间节流由 DMS 轮询节奏承担"
    ),
    cadence="随 ingest-dms 轮询（DmsConfig.loop_interval_seconds）",
)
def collect_n_consistency(
    redis: redis_lib.Redis, ex_session: Session, store: ControlPlaneStore
) -> list[LogEvent]:
    """逐 active space 采集比对（space 枚举走 ControlPlaneStore 抽象）。

    单 space 失败（EX keyspace 不存在等）跳过该 space，不阻塞其他 space。
    """
    events: list[LogEvent] = []
    for space_id in store.list_spaces():
        raw = redis.get(n_key(space_id))
        redis_n = int(raw) if raw is not None else None
        try:
            ks = keyspace_name(space_id)
            row = ex_session.execute(f"SELECT MAX(n) AS mx FROM {ks}.{EXPERIENCE_TABLE}").one()
        except Exception:  # keyspace/表不存在等：跳过本 space，不阻塞其他 space
            continue
        ex_max = row.mx if row and row.mx is not None else None
        event = check_n_consistency(redis_n, ex_max, space_id=space_id)
        if event is not None:
            events.append(event)
    return events
