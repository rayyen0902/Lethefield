"""授权注册表 store 已下沉至 lethefield_clients.auth_registry（M11）。

本模块仅作向后兼容 re-export（ops CLI 与既有 import 路径零改动）。
"""

from lethefield_clients.auth_registry import (
    AuthEntry,
    AuthRegistryStore,
    AuthScope,
    AuthStatus,
)

__all__ = ["AuthEntry", "AuthRegistryStore", "AuthScope", "AuthStatus"]
