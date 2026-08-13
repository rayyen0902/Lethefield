"""IS 签发性单测（M16）：debug 闸门、scope 白名单、claims 结构、吊销联动。

无栈：CredentialStore 用内存 stub（PG 交互路径归集成测试 test_m16_is.py）。
"""

import jwt
import pytest
from lethefield_is import tokens


class FakeCredentialStore:
    """内存 stub：与 CredentialStore 同接口（record/revoke/is_revoked）。"""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def record(self, *, jti, account_id, space_ids, agent_actor_id, scopes, internal, expires_at):
        self.rows[jti] = {
            "account_id": account_id,
            "space_ids": space_ids,
            "agent_actor_id": agent_actor_id,
            "scopes": scopes,
            "internal": internal,
            "status": "active",
            "expires_at": expires_at,
        }

    def revoke(self, jti: str) -> bool:
        if jti not in self.rows:
            return False
        self.rows[jti]["status"] = "revoked"
        return True

    def is_revoked(self, jti: str) -> bool:
        row = self.rows.get(jti)
        return row is None or row["status"] != "active"


@pytest.fixture
def store() -> FakeCredentialStore:
    return FakeCredentialStore()


def _issue(store: FakeCredentialStore, **overrides) -> str:
    kwargs = {
        "account_id": "acct_1",
        "space_ids": ["space_a"],
        "agent_actor_id": "writer_1",
        "scopes": ["record", "retrieve"],
    }
    kwargs.update(overrides)
    return tokens.issue_token(store, **kwargs)


def test_issue_roundtrip_via_api_verifier(store, monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    from lethefield_api.auth import verify_token

    token = _issue(store)
    claims = verify_token(token, is_revoked=store.is_revoked)
    assert claims.account_id == "acct_1"
    assert claims.space_ids == ("space_a",)
    assert claims.agent_actor_id == "writer_1"
    assert claims.scopes == ("record", "retrieve")


def test_claims_carry_registered_jti_exp_iat(store, monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    token = _issue(store)
    payload = jwt.decode(token, "is-unit-secret", algorithms=["HS256"])
    assert payload["jti"] in store.rows  # 先落吊销列表行再签名
    assert payload["exp"] > payload["iat"]


def test_revoked_token_rejected(store, monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    from lethefield_api.auth import verify_token
    from lethefield_api.errors import ApiError

    token = _issue(store)
    jti = jwt.decode(token, "is-unit-secret", algorithms=["HS256"])["jti"]
    assert store.revoke(jti)
    with pytest.raises(ApiError, match="凭证已吊销"):
        verify_token(token, is_revoked=store.is_revoked)


def test_legacy_token_without_jti_skips_revocation(store, monkeypatch):
    """无 jti 的旧 dev token 跳过吊销检查（向后兼容，修订记录 24①）。"""
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    from lethefield_api.auth import verify_token

    legacy = jwt.encode(
        {
            "account_id": "acct_1",
            "space_id": ["space_a"],
            "agent_actor_id": "writer_1",
            "scope": ["retrieve"],
        },
        "is-unit-secret",
        algorithm="HS256",
    )
    # checker 对未知 jti fail-closed 为 revoked，但无 jti 根本不应被调用
    claims = verify_token(legacy, is_revoked=lambda jti: True)
    assert claims.account_id == "acct_1"


def test_debug_scope_requires_internal(store, monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    with pytest.raises(ValueError, match="debug scope 仅内部签发"):
        _issue(store, scopes=["retrieve", "debug"])
    token = _issue(store, scopes=["retrieve", "debug"], internal=True)
    payload = jwt.decode(token, "is-unit-secret", algorithms=["HS256"])
    assert "debug" in payload["scope"]
    assert store.rows[payload["jti"]]["internal"] is True


def test_unknown_scope_rejected(store):
    with pytest.raises(ValueError, match="未知 scope"):
        _issue(store, scopes=["retrieve", "admin"])


def test_ttl_default_and_override(store, monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", "is-unit-secret")
    token = _issue(store)
    payload = jwt.decode(token, "is-unit-secret", algorithms=["HS256"])
    assert payload["exp"] - payload["iat"] == tokens.DEFAULT_TTL_SECONDS

    token2 = _issue(store, ttl_seconds=60)
    payload2 = jwt.decode(token2, "is-unit-secret", algorithms=["HS256"])
    assert payload2["exp"] - payload2["iat"] == 60

    monkeypatch.setenv("LETHEFIELD_IS_TOKEN_TTL_SECONDS", "120")
    token3 = _issue(store)
    payload3 = jwt.decode(token3, "is-unit-secret", algorithms=["HS256"])
    assert payload3["exp"] - payload3["iat"] == 120
