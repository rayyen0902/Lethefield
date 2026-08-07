"""管道活性探针（page 级）：broker → BookKeeper → consumer ack 端到端活性。

probe topic 在独立监控 tenant（`lethefield-monitoring/ops/ingest-probe`），
与数据面物理分离——严禁向任何 space 的 namespace 发探针（会污染数据面）。

probe_pipeline 是纯判定函数：收发往返以注入 callable 表达，单测用 fake 即可
覆盖成功/超时两路，不需要真起 Pulsar。pulsar_probe_roundtrip 是真实往返实现。
"""

import time
from collections.abc import Callable

import requests
from lethefield_clients import factories
from lethefield_logschema import LogEvent

from lethefield_ingest_dms.config import DmsConfig

SERVICE = "ingest-dms"

# 独立监控 tenant/namespace（与业务 tenant `lethefield`、训练 tenant `lethefield-training` 隔离）
MONITORING_TENANT = "lethefield-monitoring"
MONITORING_NAMESPACE = "ops"
PROBE_TOPIC_NAME = "ingest-probe"
PROBE_TOPIC = f"persistent://{MONITORING_TENANT}/{MONITORING_NAMESPACE}/{PROBE_TOPIC_NAME}"
# 探针自消费订阅名（独占：每轮一条，ack 后不留 backlog）
PROBE_SUBSCRIPTION = "ingest-dms-probe"

# tenant/namespace 幂等确保的成功状态码（409 = 已存在，与 204 同等视为成功）
_OK_STATUSES = frozenset({200, 204, 409})


def ensure_monitoring_topic(
    admin_url: str,
    *,
    http_get: Callable[..., requests.Response] = requests.get,
    http_put: Callable[..., requests.Response] = requests.put,
) -> None:
    """启动时幂等确保监控 tenant/namespace 存在（topic 由首次生产自动创建）。

    tenant 的 allowedClusters 取 GET /admin/v2/clusters 的结果（本地单集群即 standalone）。
    PUT 返回 204（建成）或 409（已存在）均视为成功；其余状态码 fail-closed 抛错。
    """
    clusters = http_get(f"{admin_url}/admin/v2/clusters", timeout=10).json()
    tenant = http_put(
        f"{admin_url}/admin/v2/tenants/{MONITORING_TENANT}",
        json={"adminRoles": [], "allowedClusters": clusters},
        timeout=10,
    )
    if tenant.status_code not in _OK_STATUSES:
        raise RuntimeError(f"建监控 tenant 失败：HTTP {tenant.status_code} {tenant.text}")
    namespace = http_put(
        f"{admin_url}/admin/v2/namespaces/{MONITORING_TENANT}/{MONITORING_NAMESPACE}",
        timeout=10,
    )
    if namespace.status_code not in _OK_STATUSES:
        raise RuntimeError(f"建监控 namespace 失败：HTTP {namespace.status_code} {namespace.text}")


def probe_pipeline(roundtrip: Callable[[], None]) -> list[LogEvent]:
    """管道活性判定：roundtrip 正常返回 = 无告警；任何异常（超时/失败）= page 级告警。"""
    try:
        roundtrip()
    except Exception as exc:
        return [
            LogEvent(
                service=SERVICE,
                event_type="ingest_probe_failed",
                payload={"level": "page", "topic": PROBE_TOPIC, "error": str(exc)},
            )
        ]
    return []


def pulsar_probe_roundtrip(config: DmsConfig) -> None:
    """真实探针往返：向 probe topic 发一条消息并自消费 ack（超时由 probe_timeout_ms 控制）。"""
    client = factories.pulsar_client()
    try:
        producer = client.create_producer(PROBE_TOPIC)
        consumer = client.subscribe(PROBE_TOPIC, subscription_name=PROBE_SUBSCRIPTION)
        try:
            payload = f"ingest-dms-probe:{time.time_ns()}".encode()
            producer.send(payload)
            message = consumer.receive(timeout_millis=config.probe_timeout_ms)
            if message.data() != payload:
                # 收到的是历史残留消息：ack 清掉后按本次探针未达处理（不静默视为成功）
                consumer.acknowledge(message)
                raise RuntimeError("探针往返收到非本次消息（订阅存在历史残留）")
            consumer.acknowledge(message)
        finally:
            producer.close()
            consumer.close()
    finally:
        client.close()
