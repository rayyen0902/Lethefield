"""客户端连接工厂。

连接参数走环境变量，函数参数可覆盖（测试注入用）。
工厂只负责构造客户端对象，真正的连接健康检查由 scripts/wait_for_stack.sh 承担。
"""

import os

import redis
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client as GremlinClient
from psycopg import Connection, connect
from pulsar import Client

_ENV = {
    "cassandra_hosts": ("LETHEFIELD_CASSANDRA_HOSTS", "localhost"),
    "cassandra_port": ("LETHEFIELD_CASSANDRA_PORT", "9042"),
    "es_url": ("LETHEFIELD_ES_URL", "http://localhost:9200"),
    "gremlin_url": ("LETHEFIELD_GREMLIN_URL", "ws://localhost:8182/gremlin"),
    "pulsar_url": ("LETHEFIELD_PULSAR_URL", "pulsar://localhost:6650"),
    "redis_url": ("LETHEFIELD_REDIS_URL", "redis://localhost:6379"),
    "pg_dsn": ("LETHEFIELD_PG_DSN", "postgresql://lethefield:lethefield@localhost:5432/lethefield"),
}


def _env(key: str) -> str:
    env_name, default = _ENV[key]
    return os.environ.get(env_name, default)


def parse_hosts(hosts: str) -> list[str]:
    """解析逗号分隔的主机列表，去空白、去空项。"""
    return [h.strip() for h in hosts.split(",") if h.strip()]


def cassandra_cluster(hosts: str | None = None, port: int | None = None) -> Cluster:
    """构造 Cassandra Cluster（未连接；调用方 .connect() 时建立连接）。"""
    return Cluster(
        contact_points=parse_hosts(hosts or _env("cassandra_hosts")),
        port=port or int(_env("cassandra_port")),
    )


def es_client(url: str | None = None) -> Elasticsearch:
    return Elasticsearch(url or _env("es_url"))


def gremlin_client(
    url: str | None = None, alias: str = "ConfigurationManagementGraph"
) -> GremlinClient:
    """构造 Gremlin 脚本提交客户端。

    自定义 server yaml 只绑定 ConfigurationManagementGraph（动态图经
    ConfiguredGraphFactory 管理），别名只需能解析，纯脚本提交用不到它。
    """
    return GremlinClient(url or _env("gremlin_url"), alias)


def pulsar_client(url: str | None = None) -> Client:
    return Client(url or _env("pulsar_url"))


def redis_client(url: str | None = None) -> redis.Redis:
    return redis.Redis.from_url(url or _env("redis_url"))


def pg_connection(dsn: str | None = None, **kwargs) -> Connection:
    return connect(dsn or _env("pg_dsn"), **kwargs)
