"""EX 事件序号原语（契约 1 的共享部分，单点定义）。

M6 从 services/api ex_ingest 迁入：FS sweep 等模块需要 n_now，而项目约定
"共享代码只允许 libs/ 三样"、禁止服务间互相 import，因此 n 分配语义、
EX keyspace 命名约定与 Redis 键约定必须收敛到本模块单点。

定案语义（M5 冻结契约 1，迁移不改任何行为）：
- 每 space 一个 keyspace `ex_{space_id}`，落 cassandra-ex 集群（M1 物理隔离红线）。
- **只有经验事件推进 n**（Redis `INCR ex:n:{space}` 分配，space 级单调）；
  元事件不分配 n——否则"用得越多忘得越快"，语义反转（设计文档 §13.2）。
- n_now 读取：Redis 缓存优先，失效时从 EX `MAX(n)` 重建。

M7 起本模块同时是 EX 读访问层（ExEvent/MetaEvent + list_*）：消费方
（corrections/rebuild 等）经此读 EX，不裸写 CQL——与 archive.py 同规约。
"""

from dataclasses import dataclass
from datetime import datetime

import redis as redis_lib
from cassandra.cluster import Session

from lethefield_clients.spaces import validate_space_id

EX_KEYSPACE_PREFIX = "ex_"

# 经验事件表名（表结构 DDL 单点在本模块 ensure_ex_keyspace，M9 从 ex_ingest 迁入——
# 调度器开通流水线需要 EX 步而不依赖 API 服务；ex_ingest 改委托 + re-export）
EXPERIENCE_TABLE = "experience_events"
# 元事件表名（表单点定义在此；ex_ingest 经此处 import，M7 起消费方共用）
META_TABLE = "meta_events"


def keyspace_name(space_id: str) -> str:
    """EX keyspace 名。space_id 字符集约束单点在 spaces.validate_space_id（M8 定案），
    fail-closed：不合法直接拒绝，不静默改写（防两个 space 映射到同一 keyspace）。"""
    validate_space_id(space_id)
    return f"{EX_KEYSPACE_PREFIX}{space_id}"


def ensure_ex_keyspace(session: Session, space_id: str) -> None:
    """幂等建 EX keyspace + 两表（M9 起为调度器开通流水线 EX 步的实现点）。"""
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


def n_key(space_id: str) -> str:
    """该 space 当前事件序号的 Redis 键（INCR 分配点与本读取共用此约定）。"""
    return f"ex:n:{space_id}"


def n_now(redis: redis_lib.Redis, session: Session, *, space_id: str) -> int:
    """读取该 space 当前事件序号：Redis 缓存优先，失效时从 EX MAX(n) 重建。"""
    cached = redis.get(n_key(space_id))
    if cached is not None:
        return int(cached)
    ks = keyspace_name(space_id)
    row = session.execute(f"SELECT MAX(n) AS mx FROM {ks}.{EXPERIENCE_TABLE}").one()
    current = row.mx if row and row.mx is not None else 0
    redis.set(n_key(space_id), current)
    return current


# ---------------------------------------------------------------- EX 读访问层（M7）

# 消费方（M7 corrections/rebuild 等）经本模块读 EX，不裸写 CQL——与 archive.py 同规约。
# 单 space keyspace 内全表扫（1.0 规模从简；space 内扫描不违红线 1，跨 space 枚举
# 走 ControlPlaneStore.list_spaces，不在本层）。


@dataclass(frozen=True)
class ExEvent:
    """EX 经验事件行（契约 1 表结构的读取映射）。"""

    n: int
    event_id: str
    content: str
    agent_actor_id: str | None
    account_id: str | None
    tau_ms: int | None
    ref_conflict: str | None
    created_at: datetime


@dataclass(frozen=True)
class MetaEvent:
    """EX 元事件行（reinforce 等，不推进 n；count 为时间窗合并的累计值）。"""

    node_key: str
    created_at: datetime
    event_id: str
    meta_type: str
    count: int
    n_at_event: int | None
    agent_actor_id: str | None
    account_id: str | None


def list_experience_events(session: Session, *, space_id: str) -> list[ExEvent]:
    """读回该 space 全部经验事件（按 n 升序——重放顺序即 n 序）。"""
    ks = keyspace_name(space_id)
    rows = session.execute(
        f"SELECT n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, "
        f"created_at FROM {ks}.{EXPERIENCE_TABLE}"
    ).all()
    return sorted(
        (
            ExEvent(
                n=row.n,
                event_id=str(row.event_id),
                content=row.content,
                agent_actor_id=row.agent_actor_id,
                account_id=row.account_id,
                tau_ms=row.tau_ms,
                ref_conflict=row.ref_conflict,
                created_at=row.created_at,
            )
            for row in rows
        ),
        key=lambda e: e.n,
    )


def list_meta_events(
    session: Session, *, space_id: str, node_key: str | None = None
) -> list[MetaEvent]:
    """读回元事件（可按 node_key 过滤），按 (node_key, created_at) 升序。"""
    ks = keyspace_name(space_id)
    if node_key is not None:
        rows = session.execute(
            f"SELECT node_key, created_at, event_id, meta_type, count, n_at_event, "
            f"agent_actor_id, account_id FROM {ks}.{META_TABLE} WHERE node_key = %s",
            (node_key,),
        ).all()
    else:
        rows = session.execute(
            f"SELECT node_key, created_at, event_id, meta_type, count, n_at_event, "
            f"agent_actor_id, account_id FROM {ks}.{META_TABLE}"
        ).all()
    return sorted(
        (
            MetaEvent(
                node_key=row.node_key,
                created_at=row.created_at,
                event_id=str(row.event_id),
                meta_type=row.meta_type,
                count=row.count,
                n_at_event=row.n_at_event,
                agent_actor_id=row.agent_actor_id,
                account_id=row.account_id,
            )
            for row in rows
        ),
        key=lambda e: (e.node_key, e.created_at),
    )
