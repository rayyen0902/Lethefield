"""训练数据授权注册表的 PostgreSQL 存取（§12.4 最小实现）。

授权注册表是 M11 授权拦截的前提：加工 worker 每批次必查，
未授权 space 的第 ③④ 类数据在入 topic 前被拒。

注意：`space_ref` 为不透明哈希，不存 space_id 明文（§12.4 样本 schema 同一约定）。
授权范围粒度对应入料口：CALIBRATION = ③ 检索质量/FF 标定明细，
CONTENT_COPY = ④ 用户记忆内容副本。

归属定案：独立 Postgres 表（M0 建表，设计文档 §12.4 待决项 1 按现状收口）；
store 自 M11 起从 ops/auth_registry 下沉到本库——API 授权闸门 / 加工 worker /
ex-feed 多处共用，共享代码只允许 libs/（ops/auth_registry 保留薄 CLI）。
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from lethefield_clients.factories import pg_connection


class AuthScope(StrEnum):
    CALIBRATION = "calibration"  # 入料口 ③：检索质量与 FF 标定明细
    CONTENT_COPY = "content_copy"  # 入料口 ④：用户记忆内容副本


class AuthStatus(StrEnum):
    ACTIVE = "active"
    REVOKED = "revoked"


@dataclass(frozen=True)
class AuthEntry:
    space_ref: str
    scopes: tuple[AuthScope, ...]
    status: AuthStatus
    created_at: datetime
    updated_at: datetime


class AuthRegistryStore:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def grant(self, space_ref: str, scopes: list[AuthScope]) -> None:
        """登记授权（幂等：重复登记刷新 scopes 并恢复 active）。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO auth_registry (space_ref, scopes, status, updated_at)
                VALUES (%s, %s, 'active', now())
                ON CONFLICT (space_ref)
                DO UPDATE SET scopes = EXCLUDED.scopes,
                              status = 'active',
                              updated_at = now()
                """,
                (space_ref, [str(s) for s in scopes]),
            )

    def revoke(self, space_ref: str) -> bool:
        """撤回授权（停止新增采样的判定依据）。返回是否存在该条目。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE auth_registry SET status = 'revoked', updated_at = now() "
                "WHERE space_ref = %s",
                (space_ref,),
            )
            return cur.rowcount > 0

    def delete(self, space_ref: str) -> bool:
        """删除注册表项（整 space 销毁处置的一环，M11 契约 5 消费侧）。返回是否存在。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM auth_registry WHERE space_ref = %s", (space_ref,))
            return cur.rowcount > 0

    def get(self, space_ref: str) -> AuthEntry | None:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT space_ref, scopes, status, created_at, updated_at "
                "FROM auth_registry WHERE space_ref = %s",
                (space_ref,),
            )
            row = cur.fetchone()
        return self._to_entry(row) if row else None

    def list(self, status: AuthStatus | None = None) -> list[AuthEntry]:
        sql = "SELECT space_ref, scopes, status, created_at, updated_at FROM auth_registry"
        params: tuple = ()
        if status is not None:
            sql += " WHERE status = %s"
            params = (str(status),)
        sql += " ORDER BY space_ref"
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return [self._to_entry(row) for row in cur.fetchall()]

    def is_authorized(self, space_ref: str, scope: AuthScope) -> bool:
        """授权拦截判定（M11 生产侧闸门与加工 worker 每批次必查的入口）。"""
        entry = self.get(space_ref)
        return entry is not None and entry.status == AuthStatus.ACTIVE and scope in entry.scopes

    @staticmethod
    def _to_entry(row) -> AuthEntry:
        space_ref, scopes, status, created_at, updated_at = row
        return AuthEntry(
            space_ref=space_ref,
            scopes=tuple(AuthScope(s) for s in scopes),
            status=AuthStatus(status),
            created_at=created_at,
            updated_at=updated_at,
        )
