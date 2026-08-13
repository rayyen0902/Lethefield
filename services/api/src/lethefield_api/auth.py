"""契约 3 验证侧：JWT claim 解出与 scope/space/debug/吊销判定（签发在 M16 IS 侧）。

claim 结构（契约 3 + v1.2 修订记录第 24 条① 首次演进）：
`account_id / space_id[] / agent_actor_id / scope[]` + 标准注册 claim
`jti/exp/iat`（只加不改）。scope 白名单单点在 `lethefield_clients.credentials`
（本模块同源引用，禁双拷贝）。

纪律：
- `agent_actor_id` 只从凭证 claim 解出——请求体携带该字段一律拒绝（fail-closed，
  不静默忽略：静默会让调用方误以为声明生效，与 Dead Man's Switch 哲学同源）。
- `debug` scope 是权限开关，绑凭证不绑请求参数；C 端凭证不授予（M16 签发侧闸门
  已落实：非 internal 渠道拒签）。
- 吊销检查（M16）：token 带 `jti` 且提供了 `is_revoked` checker → 逐请求查吊销
  列表；无 `jti` 的旧 dev token 跳过（向后兼容）。checker 异常不捕获（fail-closed
  传播为 500，不静默放行）。
"""

from collections.abc import Callable
from dataclasses import dataclass

import jwt
from lethefield_clients.credentials import (
    CREDENTIAL_SCOPES,
    DEBUG_SCOPE,
    jwt_secret,
)

from lethefield_api.errors import ApiError, ErrorCode

SCOPES: frozenset[str] = CREDENTIAL_SCOPES  # 单点委托（修订记录 24④）


@dataclass(frozen=True)
class Claims:
    account_id: str
    space_ids: tuple[str, ...]
    agent_actor_id: str
    scopes: tuple[str, ...]


def _secret() -> str:
    return jwt_secret()


def verify_token(
    token: str,
    is_revoked: Callable[[str], bool] | None = None,
) -> Claims:
    """验签 + 吊销检查 + 解出 claims；任何无效形态都映射为 401 unauthorized。"""
    try:
        payload = jwt.decode(token, _secret(), algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise ApiError(ErrorCode.UNAUTHORIZED, "token 已过期") from None
    except jwt.InvalidTokenError as exc:
        raise ApiError(ErrorCode.UNAUTHORIZED, f"token 无效：{exc}") from None

    jti = payload.get("jti")
    if jti is not None and is_revoked is not None and is_revoked(jti):
        raise ApiError(ErrorCode.UNAUTHORIZED, "凭证已吊销")

    missing = {"account_id", "space_id", "agent_actor_id", "scope"} - payload.keys()
    if missing:
        raise ApiError(ErrorCode.UNAUTHORIZED, f"token 缺少必需 claim：{sorted(missing)}")

    scopes = tuple(payload["scope"])
    unknown = set(scopes) - SCOPES
    if unknown:
        raise ApiError(ErrorCode.UNAUTHORIZED, f"未知 scope：{sorted(unknown)}")
    return Claims(
        account_id=payload["account_id"],
        space_ids=tuple(payload["space_id"]),
        agent_actor_id=payload["agent_actor_id"],
        scopes=scopes,
    )


def require_scope(claims: Claims, operation: str) -> None:
    if operation not in claims.scopes:
        raise ApiError(ErrorCode.FORBIDDEN_SCOPE, f"凭证无 {operation} scope")


def require_space(claims: Claims, space_id: str) -> None:
    if space_id not in claims.space_ids:
        raise ApiError(ErrorCode.FORBIDDEN_SPACE, f"凭证不覆盖 space {space_id!r}")


def has_debug(claims: Claims) -> bool:
    return DEBUG_SCOPE in claims.scopes


def reject_actor_spoof(body: dict) -> None:
    """请求体禁止声明 agent_actor_id（即使与凭证一致也拒绝——该字段不信客户端自报）。"""
    if "agent_actor_id" in body:
        raise ApiError(
            ErrorCode.ACTOR_SPOOF,
            "agent_actor_id 禁止从请求体声明，只认凭证 claim",
        )
