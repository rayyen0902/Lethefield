"""EX 摄入最小路径（契约 1 生产侧雏形，M5；M10 扩展为完整三存储生命周期流水线）。

定案（本模块冻结）：
- 每 space 一个 keyspace `ex_{space_id}`，落 cassandra-ex 集群（与 cell 集群物理隔离，
  M1 红线；禁止并入 cell Cassandra）。
- 事件类型分层（§13.2）：**只有经验事件推进 n**（Redis `INCR ex:n:{space}` 分配，
  space 级单调）；元事件（reinforce 追加等）不分配 n、只留痕——否则"用得越多忘得越快"。
- 同步写表返回 = §9.3 定案的"等 EX 落库确认后返回"（ack）。
- 元事件 `count` 字段为时间窗合并服务（M7 起 reinforce 走窗口合并：窗口内同节点
  多次强化合并为一笔、count 累加；纠错是经验事件不经此路径，每笔精确）。
- EX→Pulsar 生产侧（M14，v1.2 修订记录第 20 条定案）：经验事件落库确认后发布到
  `lethefield/{space_id}` 的 ex-events topic——**发布失败不阻塞同步返回**（EX 是
  SoT），最终失败 page 告警 + 指标；producer 依赖显式登记在 stream_publisher 单点。

遗留加固点（M10）：Redis INCR 与 EX 写入的竞态（INCR 成功写失败会留 n 空洞——
空洞不破坏单调性，可接受）；n_now 重建与并发 INCR 的竞态。
"""

import time
import uuid
from datetime import UTC, datetime, timedelta

import redis as redis_lib
from cassandra.cluster import Session
from lethefield_clients.ex_n import (
    EXPERIENCE_TABLE,
    META_TABLE,
    append_meta_row,
    ensure_ex_keyspace,  # noqa: F401  (re-export，M9 DDL 单点迁入 ex_n)
    keyspace_name,
    n_key,
    n_now,
    touch_last_write,
)
from lethefield_clients.ex_stream import ExStreamEvent
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_event
from lethefield_metrics import counter, histogram
from prometheus_client import REGISTRY

from lethefield_api.stream_publisher import PublishError

# M12 埋点：EX 写路径耗时（source of truth 写入，§19.3 告警线）
_EX_WRITE_DURATION = histogram(
    "lethefield_ex_write_duration_seconds",
    "EX 经验事件落表耗时（不含 n 分配）",
    registry=REGISTRY,
)

# M14 埋点：EX→Pulsar 生产侧发布结果（page 告警的聚合面；space 明细走日志事件）
_EX_STREAM_PUBLISH = counter(
    "lethefield_ex_stream_publish_total",
    "EX 经验事件发布到 Pulsar 的结果计数（ok/failed）",
    ["result"],
    registry=REGISTRY,
)


def _publish_ex_event(publisher, event: ExStreamEvent) -> None:
    """落库确认后发布 ex-events 信封；失败不阻塞同步返回（EX 是 SoT），告警 + 指标。"""
    try:
        publisher.publish(event)
    except PublishError:
        _EX_STREAM_PUBLISH.labels(result="failed").inc()
        emit_event(
            LogEvent(
                service="lethefield-api",
                event_type="ex_stream_publish_failed",
                space_id=event.space_id,
                payload={"event_id": event.event_id, "n": event.n},
            )
        )
        return
    _EX_STREAM_PUBLISH.labels(result="ok").inc()


__all__ = [
    "EXPERIENCE_TABLE",
    "META_TABLE",
    "REINFORCE_MERGE_WINDOW_MS",
    "append_experience",
    "append_meta",
    "ensure_ex_keyspace",
    "keyspace_name",
    "n_now",
]

# reinforce 时间窗合并窗口（M7 定案，占位 60s，§20 待标定）：窗口内同节点多次强化
# 合并为一笔元事件（count 累加），控制 EX 写放大；重建精度降至窗口粒度，是可接受设计。
REINFORCE_MERGE_WINDOW_MS = 60_000


def append_experience(
    session: Session,
    redis: redis_lib.Redis,
    *,
    space_id: str,
    content: str,
    agent_actor_id: str,
    account_id: str,
    tau_ms: int | None = None,
    ref_conflict: str | None = None,
    publisher=None,
) -> tuple[str, int]:
    """写入经验事件：分配该 space 下一个 n，同步落表后返回（event_id, n）。

    publisher 非 None 时（M14 定案）：落库确认后发布 ex-events 信封——发布在 ack
    之后、失败不阻塞返回（EX 是 SoT；消费侧 n 连续性校验兜底补偿）。
    """
    ks = keyspace_name(space_id)
    n: int = redis.incr(n_key(space_id))  # 经验事件才推进 n（原子分配）
    event_id = uuid.uuid4()
    now = datetime.now(UTC)
    t0 = time.perf_counter()
    session.execute(
        f"INSERT INTO {ks}.{EXPERIENCE_TABLE} "
        "(n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, now),
    )
    _EX_WRITE_DURATION.observe(time.perf_counter() - t0)  # M12：EX 写路径耗时
    touch_last_write(redis, space_id, now=now)  # M10 DMS：成功摄入才刷新
    if publisher is not None:
        _publish_ex_event(
            publisher,
            ExStreamEvent(
                space_id=space_id,
                event_id=str(event_id),
                n=n,
                content=content,
                agent_actor_id=agent_actor_id,
                account_id=account_id,
                tau_ms=tau_ms,
                ref_conflict=ref_conflict,
                created_at_ms=int(now.timestamp() * 1000),
            ),
        )
    return str(event_id), n


def _merge_window_row(
    session: Session,
    ks: str,
    *,
    node_key: str,
    meta_type: str,
    now: datetime,
    window_ms: int,
):
    """查窗口内该节点最新一笔同类元事件（合并候选）；无则 None。"""
    since = now - timedelta(milliseconds=window_ms)
    return session.execute(
        f"SELECT node_key, created_at, event_id, meta_type, count FROM {ks}.{META_TABLE} "
        "WHERE node_key = %s AND created_at >= %s ORDER BY created_at DESC LIMIT 1",
        (node_key, since),
    ).one()


def append_meta(
    session: Session,
    *,
    space_id: str,
    node_key: str,
    meta_type: str,
    n_at_event: int,
    agent_actor_id: str,
    account_id: str,
    count: int = 1,
    merge_window_ms: int | None = None,
    redis: redis_lib.Redis | None = None,
) -> str:
    """追加元事件（reinforce 等）：**不分配 n**——元事件只留痕、不推进事件距离。

    时间窗合并（M7）：`merge_window_ms` 非 None 时，窗口内同节点同类事件合并为一笔——
    同主键 UPDATE（count 累加、n_at_event 刷新），不产生新行。service.reinforce
    传 `REINFORCE_MERGE_WINDOW_MS`；纠错是经验事件不经此路径（每笔必须精确）。

    `redis` 非 None 时落库成功后刷新最近写入时间（M10 DMS 写入新鲜度数据源）。
    """
    ks = keyspace_name(space_id)
    now = datetime.now(UTC)
    if merge_window_ms is not None:
        row = _merge_window_row(
            session, ks, node_key=node_key, meta_type=meta_type, now=now, window_ms=merge_window_ms
        )
        if row is not None and row.meta_type == meta_type:
            session.execute(
                f"UPDATE {ks}.{META_TABLE} SET count = %s, n_at_event = %s "
                "WHERE node_key = %s AND created_at = %s AND event_id = %s",
                (row.count + count, n_at_event, node_key, row.created_at, row.event_id),
            )
            if redis is not None:
                touch_last_write(redis, space_id, now=datetime.now(UTC))
            return str(row.event_id)
    # 窗口外/不合并：纯 INSERT 走 ex_n 单点原语（details 置空，M14 契约 1 演进后行为不变）
    event_id = append_meta_row(
        session,
        space_id=space_id,
        node_key=node_key,
        meta_type=meta_type,
        n_at_event=n_at_event,
        agent_actor_id=agent_actor_id,
        account_id=account_id,
        count=count,
    )
    if redis is not None:
        touch_last_write(redis, space_id, now=datetime.now(UTC))
    return event_id
