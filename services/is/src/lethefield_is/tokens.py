"""JWT 签发（M16，开发文档 v1.2 修订记录第 24 条①②③）。

- claims = 契约 3 四字段（account_id / space_id[] / agent_actor_id / scope[]）
  + 标准注册 claim `jti`/`exp`/`iat`（契约 3 首次演进：只加不改）。
- 吊销机制 = jti 吊销列表：先落 `is_credentials` 行再签名（吊销列表以库为准）；
  有限时效默认 24h（env `LETHEFIELD_IS_TOKEN_TTL_SECONDS` 覆盖），1.0 不做刷新
  机制，重签发即刷新。
- debug scope 闸门 fail-closed：非 internal 渠道申请 debug 一律拒签（M5 要求的
  签发侧落实）；每个写入者身份单独签发（agent_actor_id 显式传入）。
"""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
from lethefield_clients.credentials import (
    CREDENTIAL_SCOPES,
    DEBUG_SCOPE,
    CredentialStore,
    jwt_secret,
)

DEFAULT_TTL_SECONDS = 24 * 3600


def _default_ttl() -> int:
    return int(os.environ.get("LETHEFIELD_IS_TOKEN_TTL_SECONDS", DEFAULT_TTL_SECONDS))


def issue_token(
    store: CredentialStore,
    *,
    account_id: str,
    space_ids: list[str],
    agent_actor_id: str,
    scopes: list[str],
    internal: bool = False,
    ttl_seconds: int | None = None,
) -> str:
    """签发一个写入者凭证，返回 JWT 字符串。任何非法形态拒签（ValueError）。"""
    unknown = set(scopes) - CREDENTIAL_SCOPES
    if unknown:
        raise ValueError(f"未知 scope：{sorted(unknown)}（白名单 {sorted(CREDENTIAL_SCOPES)}）")
    if DEBUG_SCOPE in scopes and not internal:
        raise ValueError("debug scope 仅内部签发（--internal），C 端凭证一律不授予")

    ttl = ttl_seconds if ttl_seconds is not None else _default_ttl()
    now = datetime.now(UTC)
    expires_at = now + timedelta(seconds=ttl)
    jti = uuid4().hex

    # 先落吊销列表行再签名——先签名后落库会在崩溃窗口产出无法吊销的 token
    store.record(
        jti=jti,
        account_id=account_id,
        space_ids=space_ids,
        agent_actor_id=agent_actor_id,
        scopes=scopes,
        internal=internal,
        expires_at=expires_at,
    )
    payload = {
        "account_id": account_id,
        "space_id": list(space_ids),
        "agent_actor_id": agent_actor_id,
        "scope": list(scopes),
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, jwt_secret(), algorithm="HS256")
