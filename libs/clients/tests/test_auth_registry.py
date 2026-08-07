"""授权注册表下沉 libs/clients 后的兼容性单测（PG 交互路径在集成测试）。"""


def test_ops_reexport_compat():
    # M11 store 下沉后 ops 包 re-export 保持既有 import 路径可用
    from lethefield_auth_registry import (
        AuthEntry,
        AuthRegistryStore,
        AuthScope,
        AuthStatus,
    )
    from lethefield_clients.auth_registry import (
        AuthRegistryStore as LibStore,
    )

    assert AuthRegistryStore is LibStore
    assert AuthScope.CALIBRATION.value == "calibration"
    assert AuthScope.CONTENT_COPY.value == "content_copy"
    assert AuthStatus.ACTIVE.value == "active"
    assert AuthEntry is not None


def test_store_has_delete():
    from lethefield_clients.auth_registry import AuthRegistryStore

    assert callable(AuthRegistryStore.delete)
