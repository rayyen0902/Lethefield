"""凭证 store / scope 白名单的轻量单测（PG 交互路径在集成测试 test_m16_is.py）。"""


def test_scope_whitelist_single_point():
    # 修订记录 24④：scope 白名单单点在 libs/clients/credentials，
    # api.auth 同源引用（禁双拷贝）
    from lethefield_api.auth import DEBUG_SCOPE as API_DEBUG_SCOPE
    from lethefield_api.auth import SCOPES as API_SCOPES
    from lethefield_clients.credentials import (
        CREDENTIAL_SCOPES,
        DEBUG_SCOPE,
    )

    assert API_SCOPES is CREDENTIAL_SCOPES
    assert API_DEBUG_SCOPE is DEBUG_SCOPE
    assert (
        frozenset({"record", "reinforce", "flag_conflict", "retrieve", "debug"})
        == CREDENTIAL_SCOPES
    )


def test_store_interface():
    from lethefield_clients.credentials import (
        CredentialRecord,
        CredentialStatus,
        CredentialStore,
    )

    for method in ("record", "revoke", "is_revoked", "get", "list"):
        assert callable(getattr(CredentialStore, method))
    assert CredentialStatus.ACTIVE.value == "active"
    assert CredentialStatus.REVOKED.value == "revoked"
    assert CredentialRecord is not None
