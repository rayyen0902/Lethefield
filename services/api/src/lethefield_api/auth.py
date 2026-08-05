"""契约 3 验证侧：JWT claim 解出与 scope/space/debug 判定（签发与吊销属 M16）。

claim 结构（契约 3）：`account_id / space_id[] / agent_actor_id / scope[]`；
scope 取值 `record | reinforce | flag_conflict | retrieve | debug`。

纪律：
- `agent_actor_id` 只从凭证 claim 解出——请求体携带该字段一律拒绝（fail-closed，
  不静默忽略：静默会让调用方误以为声明生效，与 Dead Man's Switch 哲学同源）。
- `debug` scope 是权限开关，绑凭证不绑请求参数；C 端凭证不授予（M16 签发侧落实）。
"""

import os
from dataclasses import dataclass

import jwt

from lethefield_api.errors import ApiError, ErrorCode

SCOPES: frozenset[str] = frozenset({"record", "reinforce", "flag_conflict", "retrieve", "debug"})
DEBUG_SCOPE = "debug"

# dev 默认密钥仅供本地/CI（M16 落地前）；生产部署必须经 LETHEFIELD_JWT_SECRET 注入
_DEFAULT_SECRET = "lethefield-dev-insecure-secret"


@dataclass(frozen=True)
class Claims:
    account_id: str
    space_ids: tuple[str, ...]
    agent_actor_id: str
    scopes: tuple[str, ...]


def _secret() -> str:
    return os.environ.get("LETHEFIELD_JWT_SECRET", _DEFAULT_SECRET)


def verify_token(token: str) -> Claims:
    """验签 + 解出 claims；任何无效形态都映射为 401 unauthorized。"""
    try:
        payload = jwt.decode(token, _secret(), algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise ApiError(ErrorCode.UNAUTHORIZED, "token 已过期") from None
    except jwt.InvalidTokenError as exc:
        raise ApiError(ErrorCode.UNAUTHORIZED, f"token 无效：{exc}") from None

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
