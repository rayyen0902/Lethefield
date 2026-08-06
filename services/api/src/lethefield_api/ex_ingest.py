"""EX 摄入最小路径（契约 1 生产侧雏形，M5；M10 扩展为完整三存储生命周期流水线）。

定案（本模块冻结）：
- 每 space 一个 keyspace `ex_{space_id}`，落 cassandra-ex 集群（与 cell 集群物理隔离，
  M1 红线；禁止并入 cell Cassandra）。
- 事件类型分层（§13.2）：**只有经验事件推进 n**（Redis `INCR ex:n:{space}` 分配，
  space 级单调）；元事件（reinforce 追加等）不分配 n、只留痕——否则"用得越多忘得越快"。
- 同步写表返回 = §9.3 定案的"等 EX 落库确认后返回"（ack）。
- 元事件 `count` 字段为时间窗合并服务（M7 起 reinforce 走窗口合并：窗口内同节点
  多次强化合并为一笔、count 累加；纠错是经验事件不经此路径，每笔精确）。

遗留加固点（M10）：Redis INCR 与 EX 写入的竞态（INCR 成功写失败会留 n 空洞——
空洞不破坏单调性，可接受）；n_now 重建与并发 INCR 的竞态。
"""

import uuid
from datetime import UTC, datetime, timedelta

import redis as redis_lib
from cassandra.cluster import Session
from lethefield_clients.ex_n import EXPERIENCE_TABLE, META_TABLE, keyspace_name, n_key, n_now

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


def ensure_ex_keyspace(session: Session, space_id: str) -> None:
    """幂等建 EX keyspace + 两表（M10 开通流水线 EX 步的雏形，届时直接复用）。"""
    ks = keyspace_name(space_id)
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {ks} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    # 经验事件：n 即主键（space 内单调），MAX(n) 可用于 n_now 重建
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ks}.{EXPERIENCE_TABLE} (
            n bigint PRIMARY KEY,
            event_id uuid,
            content text,
            agent_actor_id text,
            account_id text,
            tau_ms bigint,
            ref_conflict text,
            created_at timestamp
        )
        """
    )
    # 元事件：按 node_key 分区（M7 合并器按节点查窗口内 reinforce），不持有 n
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ks}.{META_TABLE} (
            node_key text,
            created_at timestamp,
            event_id uuid,
            meta_type text,
            count int,
            n_at_event bigint,
            agent_actor_id text,
            account_id text,
            PRIMARY KEY ((node_key), created_at, event_id)
        )
        """
    )


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
) -> tuple[str, int]:
    """写入经验事件：分配该 space 下一个 n，同步落表后返回（event_id, n）。"""
    ks = keyspace_name(space_id)
    n: int = redis.incr(n_key(space_id))  # 经验事件才推进 n（原子分配）
    event_id = uuid.uuid4()
    session.execute(
        f"INSERT INTO {ks}.{EXPERIENCE_TABLE} "
        "(n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, datetime.now(UTC)),
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
) -> str:
    """追加元事件（reinforce 等）：**不分配 n**——元事件只留痕、不推进事件距离。

    时间窗合并（M7）：`merge_window_ms` 非 None 时，窗口内同节点同类事件合并为一笔——
    同主键 UPDATE（count 累加、n_at_event 刷新），不产生新行。service.reinforce
    传 `REINFORCE_MERGE_WINDOW_MS`；纠错是经验事件不经此路径（每笔必须精确）。
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
            return str(row.event_id)
    event_id = uuid.uuid4()
    session.execute(
        f"INSERT INTO {ks}.{META_TABLE} "
        "(node_key, created_at, event_id, meta_type, count, n_at_event, "
        "agent_actor_id, account_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            node_key,
            now,
            event_id,
            meta_type,
            count,
            n_at_event,
            agent_actor_id,
            account_id,
        ),
    )
    return str(event_id)
