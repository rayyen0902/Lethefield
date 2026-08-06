"""存储与消息客户端封装 + ControlPlaneStore 抽象接口。

设计依据：开发文档 M0 任务 2——共享库只放三样，本库是其中之一；
所有模块的连接管理强制复用本库，禁止各写一套。
"""

from lethefield_clients.archive import (
    ARCHIVE_TABLE,
    ensure_archive_table,
    list_archived,
    write_archive,
)
from lethefield_clients.control_plane import (
    CellInfo,
    ControlPlaneStore,
    ExKeyspaceControlPlaneStore,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
    StaticControlPlaneStore,
    Tier,
    WatermarkState,
)
from lethefield_clients.ex_n import (
    EXPERIENCE_TABLE,
    META_TABLE,
    ExEvent,
    MetaEvent,
    keyspace_name,
    list_experience_events,
    list_meta_events,
    n_key,
    n_now,
)
from lethefield_clients.factories import (
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    pg_connection,
    pulsar_client,
    redis_client,
)

__all__ = [
    "ARCHIVE_TABLE",
    "EXPERIENCE_TABLE",
    "META_TABLE",
    "CellInfo",
    "ControlPlaneStore",
    "ExEvent",
    "ExKeyspaceControlPlaneStore",
    "MetaEvent",
    "SpaceMapping",
    "SpaceNotFoundError",
    "SpaceStatus",
    "StaticControlPlaneStore",
    "Tier",
    "WatermarkState",
    "cassandra_cluster",
    "ensure_archive_table",
    "es_client",
    "ex_cassandra_cluster",
    "gremlin_client",
    "keyspace_name",
    "list_archived",
    "list_experience_events",
    "list_meta_events",
    "n_key",
    "n_now",
    "pg_connection",
    "pulsar_client",
    "redis_client",
    "write_archive",
]
