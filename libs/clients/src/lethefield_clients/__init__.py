"""存储与消息客户端封装 + ControlPlaneStore 抽象接口。

设计依据：开发文档 M0 任务 2——共享库只放三样，本库是其中之一；
所有模块的连接管理强制复用本库，禁止各写一套。
"""

from lethefield_clients.control_plane import (
    CellInfo,
    ControlPlaneStore,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
    StaticControlPlaneStore,
    Tier,
    WatermarkState,
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
    "CellInfo",
    "ControlPlaneStore",
    "SpaceMapping",
    "SpaceNotFoundError",
    "SpaceStatus",
    "StaticControlPlaneStore",
    "Tier",
    "WatermarkState",
    "cassandra_cluster",
    "es_client",
    "ex_cassandra_cluster",
    "gremlin_client",
    "pg_connection",
    "pulsar_client",
    "redis_client",
]
