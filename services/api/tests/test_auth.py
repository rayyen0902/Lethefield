"""M5 契约 3 验证侧单元测试：JWT 解出、scope/space/debug 判定、伪造拒绝。"""

import time

import jwt
import pytest
from lethefield_api.auth import (
    Claims,
    has_debug,
    reject_actor_spoof,
    require_scope,
    require_space,
    verify_token,
)
from lethefield_api.errors import ApiError, ErrorCode

SECRET = "test-secret"


@pytest.fixture(autouse=True)
def _secret(monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", SECRET)


def make_token(
    *,
    account_id="acct-1",
    space_ids=("sp-1",),
    actor="claude-code",
    scopes=("record", "retrieve"),
    exp=None,
    secret=SECRET,
) -> str:
    return jwt.encode(
        {
            "account_id": account_id,
            "space_id": list(space_ids),
            "agent_actor_id": actor,
            "scope": list(scopes),
            "exp": exp if exp is not None else int(time.time()) + 600,
        },
        secret,
        algorithm="HS256",
    )


class TestVerifyToken:
    def test_valid_token_decodes(self):
        claims = verify_token(make_token())
        assert claims.account_id == "acct-1"
        assert claims.space_ids == ("sp-1",)
        assert claims.agent_actor_id == "claude-code"
        assert claims.scopes == ("record", "retrieve")

    def test_expired_rejected(self):
        with pytest.raises(ApiError, match="过期") as exc:
            verify_token(make_token(exp=int(time.time()) - 10))
        assert exc.value.code == ErrorCode.UNAUTHORIZED
        assert exc.value.http_status == 401

    def test_bad_signature_rejected(self):
        with pytest.raises(ApiError) as exc:
            verify_token(make_token(secret="wrong-secret"))
        assert exc.value.code == ErrorCode.UNAUTHORIZED

    def test_missing_claim_rejected(self):
        token = jwt.encode({"account_id": "a", "exp": int(time.time()) + 60}, SECRET)
        with pytest.raises(ApiError) as exc:
            verify_token(token)
        assert exc.value.code == ErrorCode.UNAUTHORIZED

    def test_unknown_scope_rejected(self):
        with pytest.raises(ApiError, match="未知 scope"):
            verify_token(make_token(scopes=("retrieve", "admin")))


class TestGuards:
    def _claims(self, scopes=("record",), space_ids=("sp-1",)) -> Claims:
        return Claims("acct-1", space_ids, "actor", scopes)

    def test_require_scope(self):
        require_scope(self._claims(), "record")  # 不抛
        with pytest.raises(ApiError) as exc:
            require_scope(self._claims(), "retrieve")
        assert exc.value.code == ErrorCode.FORBIDDEN_SCOPE
        assert exc.value.http_status == 403

    def test_require_space(self):
        require_space(self._claims(), "sp-1")  # 不抛
        with pytest.raises(ApiError) as exc:
            require_space(self._claims(), "sp-2")
        assert exc.value.code == ErrorCode.FORBIDDEN_SPACE

    def test_has_debug(self):
        assert not has_debug(self._claims())
        assert has_debug(self._claims(scopes=("retrieve", "debug")))


class TestActorSpoof:
    def test_body_field_rejected(self):
        with pytest.raises(ApiError) as exc:
            reject_actor_spoof({"space_id": "sp-1", "agent_actor_id": "forged"})
        assert exc.value.code == ErrorCode.ACTOR_SPOOF
        assert exc.value.http_status == 400

    def test_even_matching_claim_rejected(self):
        # 与凭证一致也拒绝：该字段不信客户端自报（形态上就不允许出现）
        with pytest.raises(ApiError):
            reject_actor_spoof({"agent_actor_id": "claude-code"})

    def test_clean_body_passes(self):
        reject_actor_spoof({"space_id": "sp-1", "content": "x"})  # 不抛
