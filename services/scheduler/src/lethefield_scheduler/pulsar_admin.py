"""Pulsar admin REST 封装（M9 开通/注销流水线的 Pulsar 步；M10 扩展策略与监控查询）。

每 space 一个 namespace（tenant 固定 `lethefield`，namespace = space_id，
M8 字符集四处命名共用定案）；Pulsar 是全局集群池，不随 Cell 分片（§18.2）。
只用 admin REST（python pulsar 客户端无 admin 能力）；全部幂等。

M10 新增：
- namespace 策略（retention + backlog quota）——v0.7 选 Pulsar 的核心理由
  （namespace 级配额/策略隔离）落地；训练管线 namespace 与业务流独立配置。
- topic stats 查询——DMS 的 backlog 监控与契约 5 硬约束 2（consumer 停摆告警）
  的数据源。
"""

import requests


def _check(response: requests.Response, allowed: tuple[int, ...], what: str) -> None:
    if response.status_code not in allowed:
        raise RuntimeError(f"Pulsar admin {what} 失败：HTTP {response.status_code} {response.text}")


def ensure_tenant(admin_url: str, tenant: str) -> None:
    """幂等建 tenant（allowedClusters 取集群现状，standalone 单集群即 ['standalone']）。"""
    clusters = requests.get(f"{admin_url}/admin/v2/clusters", timeout=10)
    _check(clusters, (200,), "list clusters")
    response = requests.put(
        f"{admin_url}/admin/v2/tenants/{tenant}",
        json={"adminRoles": [], "allowedClusters": clusters.json()},
        timeout=10,
    )
    _check(response, (204, 409), f"ensure tenant {tenant}")  # 409 = 已存在


def ensure_namespace(admin_url: str, tenant: str, namespace: str) -> None:
    """幂等建 namespace（tenant 不存在则先建）。"""
    ensure_tenant(admin_url, tenant)
    response = requests.put(f"{admin_url}/admin/v2/namespaces/{tenant}/{namespace}", timeout=10)
    _check(response, (204, 409), f"ensure namespace {tenant}/{namespace}")


def set_retention(
    admin_url: str, tenant: str, namespace: str, *, minutes: int, size_mb: int
) -> None:
    """设置 namespace 消息保留策略（幂等覆盖；size_mb=-1 表示不限大小）。"""
    response = requests.post(
        f"{admin_url}/admin/v2/namespaces/{tenant}/{namespace}/retention",
        json={"retentionTimeInMinutes": minutes, "retentionSizeInMB": size_mb},
        timeout=10,
    )
    _check(response, (204,), f"set retention {tenant}/{namespace}")


def set_backlog_quota(admin_url: str, tenant: str, namespace: str, *, quota_mb: int) -> None:
    """设置 namespace backlog 配额（幂等覆盖；超限 producer_request_hold——
    生产者明确失败语义与 DMS 监控的前提：背压可见，不静默丢弃）。"""
    response = requests.post(
        f"{admin_url}/admin/v2/namespaces/{tenant}/{namespace}/backlogQuota",
        json={
            "limitSize": quota_mb * 1024 * 1024,
            "limitTime": -1,
            "policy": "producer_request_hold",
        },
        timeout=10,
    )
    _check(response, (204,), f"set backlog quota {tenant}/{namespace}")


def topic_stats(admin_url: str, topic: str) -> dict:
    """查 topic 统计（subscriptions/backlog 等）。

    topic 为全限定名 `persistent://{tenant}/{namespace}/{topic}`。
    """
    prefix = "persistent://"
    if not topic.startswith(prefix):
        raise ValueError(f"topic 必须是全限定名（{prefix}...）：{topic!r}")
    path = topic[len(prefix) :]
    response = requests.get(f"{admin_url}/admin/v2/persistent/{path}/stats", timeout=10)
    _check(response, (200,), f"topic stats {topic}")
    return response.json()


def subscription_backlog(stats: dict, subscription: str) -> int | None:
    """从 topic stats 取指定 subscription 的 msgBacklog；subscription 不存在返回 None。"""
    sub = stats.get("subscriptions", {}).get(subscription)
    if sub is None:
        return None
    return int(sub.get("msgBacklog", 0))


def delete_namespace(admin_url: str, tenant: str, namespace: str) -> None:
    """幂等删 namespace（404 = 不存在，静默放行）。"""
    response = requests.delete(f"{admin_url}/admin/v2/namespaces/{tenant}/{namespace}", timeout=10)
    _check(response, (204, 404), f"delete namespace {tenant}/{namespace}")
