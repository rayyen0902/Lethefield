"""凭证存取与 scope 白名单单点（M16，开发文档 v1.2 修订记录第 24 条）。

契约 3 首次演进：JWT 新增标准注册 claim `jti`/`exp`/`iat`（四字段结构不变，
验证侧对无 `jti` 的旧 dev token 跳过吊销检查）。吊销机制 = jti 吊销列表
（本模块 `is_credentials.status`，API 验证侧逐请求检查）+ 有限时效。

scope 白名单单点化（修订记录 24④）：`CREDENTIAL_SCOPES`/`DEBUG_SCOPE` 唯一定义
在本模块，`api.auth`（验证侧）与 `lethefield_is.tokens`（签发侧）同源引用，
禁双拷贝。store 下沉 libs 的理由同 M11 auth_registry：签发侧（services/is）
与验证侧（services/api）共用，共享代码只允许 libs/。
"""

import os
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from lethefield_clients.factories import pg_connection

# 契约 3 scope 白名单（五值；debug 是权限开关，签发侧仅 internal 渠道可授）
CREDENTIAL_SCOPES: frozenset[str] = frozenset(
    {"record", "reinforce", "flag_conflict", "retrieve", "debug"}
)
DEBUG_SCOPE = "debug"

# dev 默认密钥仅供本地/CI；生产部署必须经 LETHEFIELD_JWT_SECRET 注入
_DEFAULT_SECRET = "lethefield-dev-insecure-secret"


def jwt_secret() -> str:
    """HS256 密钥解析单点（签发侧 is.tokens 与验证侧 api.auth 同源）。"""
    return os.environ.get("LETHEFIELD_JWT_SECRET", _DEFAULT_SECRET)


class CredentialStatus(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"


@dataclass(frozen=True)
class CredentialRecord:
    jti: str
    account_id: str
    space_ids: tuple[str, ...]
    agent_actor_id: str
    scopes: tuple[str, ...]
    internal: bool
    status: CredentialStatus
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None


class CredentialStore:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def record(
        self,
        *,
        jti: str,
        account_id: str,
        space_ids: list[str],
        agent_actor_id: str,
        scopes: list[str],
        internal: bool,
        expires_at: datetime,
    ) -> None:
        """登记新签发凭证（签发路径先落行再签名——吊销列表以库为准）。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO is_credentials
                    (jti, account_id, space_ids, agent_actor_id, scopes, internal,
                     status, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'active', %s)
                """,
                (jti, account_id, space_ids, agent_actor_id, scopes, internal, expires_at),
            )

    def revoke(self, jti: str) -> bool:
        """吊销凭证（幂等：重复吊销仍返回 True）。返回是否存在该凭证。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE is_credentials SET status = 'revoked', revoked_at = now() WHERE jti = %s",
                (jti,),
            )
            return cur.rowcount > 0

    def is_revoked(self, jti: str) -> bool:
        """API 验证侧吊销检查入口：查无此 jti 按已吊销论（fail-closed）。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM is_credentials WHERE jti = %s", (jti,))
            row = cur.fetchone()
        return row is None or row[0] != str(CredentialStatus.ACTIVE)

    def get(self, jti: str) -> CredentialRecord | None:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT jti, account_id, space_ids, agent_actor_id, scopes, internal, "
                "status, created_at, expires_at, revoked_at "
                "FROM is_credentials WHERE jti = %s",
                (jti,),
            )
            row = cur.fetchone()
        return self._to_record(row) if row else None

    def list(self, account_id: str | None = None) -> list[CredentialRecord]:
        sql = (
            "SELECT jti, account_id, space_ids, agent_actor_id, scopes, internal, "
            "status, created_at, expires_at, revoked_at FROM is_credentials"
        )
        params: tuple = ()
        if account_id is not None:
            sql += " WHERE account_id = %s"
            params = (account_id,)
        sql += " ORDER BY created_at"
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return [self._to_record(row) for row in cur.fetchall()]

    @staticmethod
    def _to_record(row) -> CredentialRecord:
        (
            jti,
            account_id,
            space_ids,
            agent_actor_id,
            scopes,
            internal,
            status,
            created_at,
            expires_at,
            revoked_at,
        ) = row
        return CredentialRecord(
            jti=jti,
            account_id=account_id,
            space_ids=tuple(space_ids),
            agent_actor_id=agent_actor_id,
            scopes=tuple(scopes),
            internal=internal,
            status=CredentialStatus(status),
            created_at=created_at,
            expires_at=expires_at,
            revoked_at=revoked_at,
        )
