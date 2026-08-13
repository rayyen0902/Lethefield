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

契约 1 首次演进（M14，v1.2 修订记录第 19 条定案）：`meta_events` 加可空
`details` 列（text/JSON，按 meta_type 分型、schema 单点在 lethefield_rms.schema），
承载 scoring_result 六维原始值；reinforce 等既有类型置空、行为不变。契约演进
规则自此明确：只允许"可空加列"式兼容演进；改语义/改键/改摄入路径视同契约修改。
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

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
    ensure_ex_keyspace_named(session, keyspace_name(space_id))


def ensure_ex_keyspace_named(session: Session, ks: str) -> None:
    """按显式 keyspace 名建 EX schema（M10 迁移流水线的 scratch/目标 keyspace 用）。

    DDL 单点不变——正常路径仍走 ensure_ex_keyspace（space_id 派生命名），
    本变体只服务迁移/导出工具链，不对业务路径开放。
    """
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
    # 元事件：按 node_key 分区（M7 合并器按节点查窗口内 reinforce），不持有 n。
    # details（M14 契约 1 首次演进）：可空 JSON payload，按 meta_type 分型——
    # scoring_result 存六维原始值 + 合成 s + 模型版本 + 事件引用；reinforce 置空。
    # CREATE TABLE IF NOT EXISTS 不改既有表，dev 卷 make reset 后生效。
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
            details text,
            PRIMARY KEY ((node_key), created_at, event_id)
        )
        """
    )


def n_key(space_id: str) -> str:
    """该 space 当前事件序号的 Redis 键（INCR 分配点与本读取共用此约定）。"""
    return f"ex:n:{space_id}"


def last_write_key(space_id: str) -> str:
    """该 space 最近一次成功摄入时间的 Redis 键（M10 DMS 写入新鲜度的数据源）。"""
    return f"ex:last_write:{space_id}"


def touch_last_write(redis: redis_lib.Redis, space_id: str, *, now: datetime) -> None:
    """摄入路径在事件落库确认后调用：刷新最近写入时间（ISO 格式 UTC）。

    只许摄入路径（ex_ingest）在**写入成功后**调用——DMS 的"活跃"语义锚点是
    成功摄入，不是尝试摄入（设计文档 §7.5.1：不以"看起来在运行"为证据）。
    """
    redis.set(last_write_key(space_id), now.isoformat())


def last_write_at(redis: redis_lib.Redis, space_id: str) -> datetime | None:
    """读最近成功摄入时间；从未写入返回 None（DMS 巡检读取入口）。"""
    raw = redis.get(last_write_key(space_id))
    if raw is None:
        return None
    text = raw.decode() if isinstance(raw, bytes) else raw
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


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
    """EX 元事件行（reinforce 等，不推进 n；count 为时间窗合并的累计值）。

    details（M14 契约 1 演进）：可空 JSON payload，按 meta_type 分型
    （scoring_result 的 schema 单点在 lethefield_rms.schema）；reinforce 为 None。
    """

    node_key: str
    created_at: datetime
    event_id: str
    meta_type: str
    count: int
    n_at_event: int | None
    agent_actor_id: str | None
    account_id: str | None
    details: str | None = None


_EXPERIENCE_COLUMNS = (
    "n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, created_at"
)


def _experience_row(row) -> ExEvent:
    return ExEvent(
        n=row.n,
        event_id=str(row.event_id),
        content=row.content,
        agent_actor_id=row.agent_actor_id,
        account_id=row.account_id,
        tau_ms=row.tau_ms,
        ref_conflict=row.ref_conflict,
        created_at=row.created_at,
    )


def list_experience_events(session: Session, *, space_id: str) -> list[ExEvent]:
    """读回该 space 全部经验事件（按 n 升序——重放顺序即 n 序）。"""
    ks = keyspace_name(space_id)
    rows = session.execute(f"SELECT {_EXPERIENCE_COLUMNS} FROM {ks}.{EXPERIENCE_TABLE}").all()
    return sorted((_experience_row(row) for row in rows), key=lambda e: e.n)


def get_experience_event(session: Session, *, space_id: str, n: int) -> ExEvent | None:
    """按主键点查单条经验事件；不存在返回 None。

    M15 写入链的反查取数口（修订记录第 23 条①）：ScoringResult 信封只作触发，
    c_i/τ_i/A_i 以 EX 为准——A_i 取自此行的 agent_actor_id 列（摄入层按 JWT
    claim 盖章，禁从事件体文本读）。
    """
    ks = keyspace_name(space_id)
    row = session.execute(
        f"SELECT {_EXPERIENCE_COLUMNS} FROM {ks}.{EXPERIENCE_TABLE} WHERE n = %s",
        (n,),
    ).one()
    return None if row is None else _experience_row(row)


def list_experience_events_range(
    session: Session, *, space_id: str, n_from: int, n_to: int
) -> list[ExEvent]:
    """按 n 闭区间读经验事件（按 n 升序）——M15 n 缺口补偿专用。

    n 是单行主键（partition key，无聚簇列），CQL 不支持其上的范围谓词——
    逐 n 主键点查实现（缺口区间常态很小；点查无全表扫，也不触发
    ALLOW FILTERING）。
    """
    events = []
    for n in range(n_from, n_to + 1):
        event = get_experience_event(session, space_id=space_id, n=n)
        if event is not None:
            events.append(event)
    return events


def list_meta_events(
    session: Session, *, space_id: str, node_key: str | None = None
) -> list[MetaEvent]:
    """读回元事件（可按 node_key 过滤），按 (node_key, created_at) 升序。"""
    ks = keyspace_name(space_id)
    columns = (
        "node_key, created_at, event_id, meta_type, count, n_at_event, "
        "agent_actor_id, account_id, details"
    )
    if node_key is not None:
        rows = session.execute(
            f"SELECT {columns} FROM {ks}.{META_TABLE} WHERE node_key = %s",
            (node_key,),
        ).all()
    else:
        rows = session.execute(f"SELECT {columns} FROM {ks}.{META_TABLE}").all()
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
                details=row.details,
            )
            for row in rows
        ),
        key=lambda e: (e.node_key, e.created_at),
    )


# ---------------------------------------------------------------- EX 写访问原语（M14）


def append_meta_row(
    session: Session,
    *,
    space_id: str,
    node_key: str,
    meta_type: str,
    n_at_event: int | None,
    agent_actor_id: str | None,
    account_id: str | None,
    count: int = 1,
    details: str | None = None,
) -> str:
    """追加一笔元事件（纯 INSERT，不分配 n）——EX 元事件写入的单点原语。

    时间窗合并不在本层：ex_ingest.append_meta 的 reinforce 合并逻辑自行查窗口
    后走 UPDATE；本原语服务"每笔精确"的元事件（M14 scoring_result 等）。
    """
    ks = keyspace_name(space_id)
    event_id = uuid.uuid4()
    session.execute(
        f"INSERT INTO {ks}.{META_TABLE} "
        "(node_key, created_at, event_id, meta_type, count, n_at_event, "
        "agent_actor_id, account_id, details) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            node_key,
            datetime.now(UTC),
            event_id,
            meta_type,
            count,
            n_at_event,
            agent_actor_id,
            account_id,
            details,
        ),
    )
    return str(event_id)
