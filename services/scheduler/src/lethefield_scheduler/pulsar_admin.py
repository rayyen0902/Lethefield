"""Pulsar admin REST 封装（M9 开通/注销流水线的 Pulsar 步）。

每 space 一个 namespace（tenant 固定 `lethefield`，namespace = space_id，
M8 字符集四处命名共用定案）；Pulsar 是全局集群池，不随 Cell 分片（§18.2）。
只用 admin REST（python pulsar 客户端无 admin 能力）；全部幂等。
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


def delete_namespace(admin_url: str, tenant: str, namespace: str) -> None:
    """幂等删 namespace（404 = 不存在，静默放行）。"""
    response = requests.delete(f"{admin_url}/admin/v2/namespaces/{tenant}/{namespace}", timeout=10)
    _check(response, (204, 404), f"delete namespace {tenant}/{namespace}")
