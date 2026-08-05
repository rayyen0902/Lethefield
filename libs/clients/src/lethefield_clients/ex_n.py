"""EX 事件序号原语（契约 1 的共享部分，单点定义）。

M6 从 services/api ex_ingest 迁入：FS sweep 等模块需要 n_now，而项目约定
"共享代码只允许 libs/ 三样"、禁止服务间互相 import，因此 n 分配语义、
EX keyspace 命名约定与 Redis 键约定必须收敛到本模块单点。

定案语义（M5 冻结契约 1，迁移不改任何行为）：
- 每 space 一个 keyspace `ex_{space_id}`，落 cassandra-ex 集群（M1 物理隔离红线）。
- **只有经验事件推进 n**（Redis `INCR ex:n:{space}` 分配，space 级单调）；
  元事件不分配 n——否则"用得越多忘得越快"，语义反转（设计文档 §13.2）。
- n_now 读取：Redis 缓存优先，失效时从 EX `MAX(n)` 重建。
"""

import redis as redis_lib
from cassandra.cluster import Session

EX_KEYSPACE_PREFIX = "ex_"

# 经验事件表名（表结构 DDL 仍在 ex_ingest 的 ensure_ex_keyspace，M10 扩展时同迁）
EXPERIENCE_TABLE = "experience_events"


def keyspace_name(space_id: str) -> str:
    """EX keyspace 名。space_id 字符集约束由 M8 正式定义；此处 fail-closed：
    不满足 [a-z0-9_]≤40 直接拒绝，不静默改写（防两个 space 映射到同一 keyspace）。"""
    if (
        not space_id
        or len(space_id) > 40
        or not all(c.islower() or c.isdigit() or c == "_" for c in space_id)
    ):
        raise ValueError(f"space_id {space_id!r} 不满足 EX keyspace 命名约束：[a-z0-9_]、≤40 字符")
    return f"{EX_KEYSPACE_PREFIX}{space_id}"


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
