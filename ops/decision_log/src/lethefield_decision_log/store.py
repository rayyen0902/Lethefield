"""决策留痕的 PostgreSQL 存取。"""

from dataclasses import dataclass
from datetime import datetime

from lethefield_clients import pg_connection


@dataclass(frozen=True)
class DecisionRecord:
    id: int
    created_at: datetime
    title: str
    context: str
    decision: str
    rationale: str
    decided_by: str


class DecisionLogStore:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def submit(
        self,
        title: str,
        decision: str,
        decided_by: str,
        context: str = "",
        rationale: str = "",
    ) -> int:
        """提交一条决策留痕，返回记录 id。"""
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO decision_log (title, context, decision, rationale, decided_by)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (title, context, decision, rationale, decided_by),
            )
            return cur.fetchone()[0]

    def get(self, record_id: int) -> DecisionRecord | None:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, created_at, title, context, decision, rationale, decided_by "
                "FROM decision_log WHERE id = %s",
                (record_id,),
            )
            row = cur.fetchone()
        return DecisionRecord(*row) if row else None

    def list(self, limit: int = 50) -> list[DecisionRecord]:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, created_at, title, context, decision, rationale, decided_by "
                "FROM decision_log ORDER BY id DESC LIMIT %s",
                (limit,),
            )
            return [DecisionRecord(*row) for row in cur.fetchall()]
