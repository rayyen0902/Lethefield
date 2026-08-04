"""各组件时钟采集与偏移告警判定。

collect_all 采集真实栈（compose 单节点形态）；
check_offsets 是纯判定函数，接受任意样本列表——
模拟时钟跳变的告警测试通过注入伪造样本/伪造采集器完成，不需要真的拨时钟。
"""

import subprocess
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import requests
from cassandra.cluster import Cluster
from gremlin_python.driver.client import Client
from lethefield_clients import pg_connection, redis_client

# 告警阈值：偏移绝对值超过该秒数触发告警。
# 参考依据：Cassandra LWW 对秒级偏差不敏感（毫秒级列时间戳），
# 但分钟级偏移已足以造成静默吞写（spike 实测 +68min）；
# NTP 硬化后正常偏移应在百毫秒量级，5s 已非常宽松。
DEFAULT_THRESHOLD_SECONDS = 5.0


@dataclass(frozen=True)
class OffsetSample:
    component: str
    offset_seconds: float  # 组件时钟 − 参考时钟
    remote_time: datetime
    reference_time: datetime


def check_offsets(
    samples: list[OffsetSample],
    threshold_seconds: float = DEFAULT_THRESHOLD_SECONDS,
) -> list[str]:
    """偏移告警判定：返回告警消息列表（空 = 无告警）。"""
    return [
        f"时钟偏移告警: {s.component} 偏移 {s.offset_seconds:+.1f}s，"
        f"超过阈值 ±{threshold_seconds}s（红线 6）"
        for s in samples
        if abs(s.offset_seconds) > threshold_seconds
    ]


def _sample(component: str, remote: datetime, reference: datetime) -> OffsetSample:
    return OffsetSample(
        component=component,
        offset_seconds=(remote - reference).total_seconds(),
        remote_time=remote,
        reference_time=reference,
    )


def _reference_now() -> datetime:
    """参考时钟：本机（真实部署中为 NTP 硬化的监控节点）。"""
    return datetime.now(UTC)


def pg_now() -> datetime:
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]


def redis_now() -> datetime:
    seconds, micros = redis_client().time()
    return datetime.fromtimestamp(seconds + micros / 1e6, tz=UTC)


def cassandra_now(host: str = "localhost", port: int = 9042) -> datetime:
    cluster = Cluster([host], port=port)
    try:
        session = cluster.connect()
        ms = session.execute("SELECT toUnixTimestamp(now()) FROM system.local").one()[0]
        return datetime.fromtimestamp(ms / 1000, tz=UTC)
    finally:
        cluster.shutdown()


def http_date_now(url: str) -> datetime:
    """经 HTTP Date 响应头取时钟（Pulsar admin 适用；ES 响应无 Date 头，见 container_now）。"""
    response = requests.get(url, timeout=10)
    date_header = response.headers.get("Date")
    if date_header is None:
        raise ValueError(f"{url} 响应无 Date 头，无法取其时钟")
    return parsedate_to_datetime(date_header)


def container_now(service: str) -> datetime:
    """经 docker compose exec 读容器系统时钟（ES 等无时钟 API 的组件）。

    单节点 compose 形态下容器共享 VM 时钟，此检查验证的正是"节点时钟"本身——
    红线 6 监控对象是节点时钟偏移，不是服务内部时钟。
    """
    output = subprocess.run(
        ["docker", "compose", "exec", "-T", service, "date", "+%s"],
        capture_output=True,
        text=True,
        check=True,
    )
    return datetime.fromtimestamp(int(output.stdout.strip()), tz=UTC)


def janusgraph_now() -> datetime:
    client = Client("ws://localhost:8182/gremlin", "ConfigurationManagementGraph")
    try:
        ms = client.submit("new Date().getTime()").all().result()[0]
        return datetime.fromtimestamp(ms / 1000, tz=UTC)
    finally:
        client.close()


# 采集器清单：组件名 → 采集函数。新增组件在此登记。
COLLECTORS: dict[str, Callable[[], datetime]] = {
    "postgres": pg_now,
    "redis": redis_now,
    "cassandra-cell": lambda: cassandra_now("localhost", 9042),
    "cassandra-ex": lambda: cassandra_now("localhost", 9043),
    "es-graph": lambda: container_now("es-graph"),
    "es-ops": lambda: container_now("es-ops"),
    "pulsar": lambda: http_date_now("http://localhost:8080/admin/v2/clusters"),
    "janusgraph": janusgraph_now,
}


def collect_all(
    collectors: dict[str, Callable[[], datetime]] | None = None,
) -> list[OffsetSample]:
    """采集全部组件时钟偏移。单个组件采集失败不阻塞其他组件，记为异常样本。"""
    samples = []
    for component, collector in (collectors or COLLECTORS).items():
        reference = _reference_now()
        try:
            remote = collector()
        except Exception:
            # 采集失败本身即异常信号：记为超大偏移触发告警，不静默跳过
            failed = replace(_sample(component, reference, reference), offset_seconds=float("inf"))
            samples.append(failed)
            continue
        samples.append(_sample(component, remote, reference))
    return samples
