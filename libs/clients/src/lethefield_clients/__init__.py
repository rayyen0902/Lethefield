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
from lethefield_clients.control_backup import export_jsonl, restore_jsonl
from lethefield_clients.control_plane import (
    CONTROL_KEYSPACE,
    LOCAL_CELL_ID,
    CellInfo,
    ControlPlaneStore,
    MappingTableControlPlaneStore,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
    StaticControlPlaneStore,
    Tier,
    WatermarkState,
    local_cell,
)
from lethefield_clients.ex_n import (
    EXPERIENCE_TABLE,
    META_TABLE,
    ExEvent,
    MetaEvent,
    ensure_ex_keyspace,
    ensure_ex_keyspace_named,
    keyspace_name,
    last_write_at,
    last_write_key,
    list_experience_events,
    list_meta_events,
    n_key,
    n_now,
    touch_last_write,
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
from lethefield_clients.mapping_cache import MappingCache
from lethefield_clients.spaces import SPACE_ID_MAX_LEN, SpaceType, validate_space_id
from lethefield_clients.training_control import (
    CONTROL_NAMESPACE,
    DESTROY_TOPIC,
    TRAINING_TENANT,
    SpaceDestroyCommand,
    control_topic,
    space_ref_of,
)

__all__ = [
    "ARCHIVE_TABLE",
    "CONTROL_KEYSPACE",
    "CONTROL_NAMESPACE",
    "DESTROY_TOPIC",
    "EXPERIENCE_TABLE",
    "LOCAL_CELL_ID",
    "META_TABLE",
    "SPACE_ID_MAX_LEN",
    "TRAINING_TENANT",
    "CellInfo",
    "ControlPlaneStore",
    "ExEvent",
    "MappingCache",
    "MappingTableControlPlaneStore",
    "MetaEvent",
    "SpaceDestroyCommand",
    "SpaceMapping",
    "SpaceNotFoundError",
    "SpaceStatus",
    "SpaceType",
    "StaticControlPlaneStore",
    "Tier",
    "WatermarkState",
    "cassandra_cluster",
    "control_topic",
    "ensure_archive_table",
    "ensure_ex_keyspace",
    "ensure_ex_keyspace_named",
    "es_client",
    "ex_cassandra_cluster",
    "export_jsonl",
    "gremlin_client",
    "keyspace_name",
    "last_write_at",
    "last_write_key",
    "list_archived",
    "list_experience_events",
    "list_meta_events",
    "local_cell",
    "n_key",
    "n_now",
    "pg_connection",
    "pulsar_client",
    "redis_client",
    "restore_jsonl",
    "space_ref_of",
    "touch_last_write",
    "validate_space_id",
    "write_archive",
]
