"""账号与归属关系的 PostgreSQL 存取（M16，开发文档 §17）。

账号/归属两表只被 IS 服务使用，故留在本服务（凭证表 API 验证侧也要查，
已下沉 libs/clients/credentials.py——共享代码只允许 libs/ 的边界）。
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from lethefield_clients.factories import pg_connection


class AccountStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


@dataclass(frozen=True)
class Account:
    account_id: str
    display_name: str
    status: AccountStatus
    created_at: datetime
    updated_at: datetime


class IsStore:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def create_account(self, account_id: str, display_name: str = "") -> None:
        """开户（幂等：已存在刷新 display_name，不重置 status）。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO is_accounts (account_id, display_name)
                VALUES (%s, %s)
                ON CONFLICT (account_id)
                DO UPDATE SET display_name = EXCLUDED.display_name,
                              updated_at = now()
                """,
                (account_id, display_name),
            )

    def get_account(self, account_id: str) -> Account | None:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, display_name, status, created_at, updated_at "
                "FROM is_accounts WHERE account_id = %s",
                (account_id,),
            )
            row = cur.fetchone()
        return self._to_account(row) if row else None

    def list_accounts(self) -> list[Account]:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT account_id, display_name, status, created_at, updated_at "
                "FROM is_accounts ORDER BY account_id"
            )
            return [self._to_account(row) for row in cur.fetchall()]

    def disable_account(self, account_id: str) -> bool:
        """停用账号（签发侧拒签 disabled 账号；已签发凭证吊销走 credential revoke）。
        返回是否存在该账号。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE is_accounts SET status = 'disabled', updated_at = now() "
                "WHERE account_id = %s",
                (account_id,),
            )
            return cur.rowcount > 0

    def bind_space(self, account_id: str, space_id: str) -> None:
        """登记归属（幂等）。只在 provision 成功后调用（无半开通状态）。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO is_space_owners (account_id, space_id) "
                "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (account_id, space_id),
            )

    def list_spaces_of(self, account_id: str) -> list[str]:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT space_id FROM is_space_owners WHERE account_id = %s ORDER BY space_id",
                (account_id,),
            )
            return [row[0] for row in cur.fetchall()]

    @staticmethod
    def _to_account(row) -> Account:
        account_id, display_name, status, created_at, updated_at = row
        return Account(
            account_id=account_id,
            display_name=display_name,
            status=AccountStatus(status),
            created_at=created_at,
            updated_at=updated_at,
        )
