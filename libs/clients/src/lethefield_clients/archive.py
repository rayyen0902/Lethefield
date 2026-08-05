"""归档副本访问封装（M6 定案载体）。

定案（开发文档 M6，升级确认）：归档副本（节点字段 + 图邻接快照）写入
**本 space 自己的 RMS keyspace** 内专用表 `archived_nodes`，直写 CQL、
不经 JanusGraph。理由：整 space 销毁随 RMS keyspace DROP 自动完成、
迁移 snapshot 自动携带；不新增 keyspace（Cell 水位维度不翻倍）；
EX 集群保持纯事件流不被派生数据污染。

表名与全部 CQL 封装于本模块，FS 等调用方不裸写 CQL 字符串。
M7 重放重建脚本经 `list_archived` 读回快照。
"""

import json
from datetime import UTC, datetime

from cassandra.cluster import Session

ARCHIVE_TABLE = "archived_nodes"


# keyspace 标识符校验（防 CQL 注入；图名 = keyspace 名，本就该是简单标识符）
def _check_keyspace(keyspace: str) -> None:
    if not keyspace or not all(c.isalnum() or c == "_" for c in keyspace):
        raise ValueError(f"非法 keyspace 名：{keyspace!r}")


def ensure_archive_table(session: Session, keyspace: str) -> None:
    """幂等建归档表（FS 首次归档某 space 时调用；per-table compaction 留待冷数据调优）。"""
    _check_keyspace(keyspace)
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{ARCHIVE_TABLE} (
            node_key text PRIMARY KEY,
            archived_at timestamp,
            snapshot text
        )
        """
    )


def write_archive(
    session: Session,
    keyspace: str,
    *,
    node_key: str,
    snapshot: dict,
    archived_at: datetime | None = None,
) -> None:
    """写入一个节点的归档副本。snapshot = {"props": {...}, "edges": [...]} 的 JSON 序列化。"""
    _check_keyspace(keyspace)
    session.execute(
        f"INSERT INTO {keyspace}.{ARCHIVE_TABLE} (node_key, archived_at, snapshot) "
        "VALUES (%s, %s, %s)",
        (node_key, archived_at or datetime.now(UTC), json.dumps(snapshot, ensure_ascii=False)),
    )


def list_archived(session: Session, keyspace: str) -> list[dict]:
    """读回该 space 全部归档副本（测试校验与 M7 重放重建用）。"""
    _check_keyspace(keyspace)
    rows = session.execute(
        f"SELECT node_key, archived_at, snapshot FROM {keyspace}.{ARCHIVE_TABLE}"
    ).all()
    return [
        {
            "node_key": row.node_key,
            "archived_at": row.archived_at,
            "snapshot": json.loads(row.snapshot),
        }
        for row in rows
    ]
