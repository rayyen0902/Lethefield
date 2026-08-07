"""决策留痕的 PostgreSQL 存取（§11.3）。

v1.2 定案三列（M0 任务 5 补齐 §11.3 既定要求）：`agent_suggestion`（Agent 建议内容）、
`outcome`（accepted|modified|rejected，人类对建议的处置结果）、
`escalation_type`（§11.2 四类，可空）——留痕表单即标注界面，不新增标注工种。

M11 入料口 ①：提交路径按 decision_rules 判定 R1（outcome≠accepted）/ R2
（escalation_type 非空），命中即经注入的 publisher 喂训练 topic（best-effort，
失败只留痕不阻塞提交——留痕库是 SoT，feed 可重放补齐）。① 类是内部决策
元数据、无用户内容，不过授权注册表闸门（闸门只管 ③④ 类，既定边界）。
"""

import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from lethefield_clients import (
    DECISION_OUTCOMES,
    ESCALATION_TYPES,
    FeedEvent,
    FeedKind,
    FeedSource,
    decision_rules,
    pg_connection,
)
from lethefield_logschema import LogEvent


@dataclass(frozen=True)
class DecisionRecord:
    id: int
    created_at: datetime
    title: str
    context: str
    decision: str
    rationale: str
    decided_by: str
    agent_suggestion: str
    outcome: str
    escalation_type: str | None


_COLUMNS = (
    "id, created_at, title, context, decision, rationale, decided_by, "
    "agent_suggestion, outcome, escalation_type"
)


class DecisionLogStore:
    def __init__(
        self,
        dsn: str | None = None,
        publish: Callable[[FeedEvent], None] | None = None,
    ) -> None:
        self._dsn = dsn
        self._publish = publish

    def submit(
        self,
        title: str,
        decision: str,
        decided_by: str,
        context: str = "",
        rationale: str = "",
        agent_suggestion: str = "",
        outcome: str = "accepted",
        escalation_type: str | None = None,
    ) -> int:
        """提交一条决策留痕，返回记录 id。R1/R2 命中时顺带喂训练 topic（① 口）。"""
        if outcome not in DECISION_OUTCOMES:
            raise ValueError(f"非法 outcome：{outcome!r}（取值 {sorted(DECISION_OUTCOMES)}）")
        if escalation_type is not None and escalation_type not in ESCALATION_TYPES:
            raise ValueError(
                f"非法 escalation_type：{escalation_type!r}（取值 {sorted(ESCALATION_TYPES)}）"
            )
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO decision_log (title, context, decision, rationale, decided_by,
                                          agent_suggestion, outcome, escalation_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    title,
                    context,
                    decision,
                    rationale,
                    decided_by,
                    agent_suggestion,
                    outcome,
                    escalation_type,
                ),
            )
            record_id = cur.fetchone()[0]
        self._feed(
            record_id,
            title,
            context,
            decision,
            rationale,
            decided_by,
            agent_suggestion,
            outcome,
            escalation_type,
        )
        return record_id

    def _feed(
        self,
        record_id: int,
        title: str,
        context: str,
        decision: str,
        rationale: str,
        decided_by: str,
        agent_suggestion: str,
        outcome: str,
        escalation_type: str | None,
    ) -> None:
        """R1/R2 命中 → 发布 decision_comparison feed；未命中不进训练管线（既定原则）。"""
        if self._publish is None or not decision_rules(outcome, escalation_type):
            return
        event = FeedEvent(
            kind=FeedKind.DECISION_COMPARISON,
            source=FeedSource.DECISION_LOG,
            space_ref=None,  # ① 类为运维元数据，与具体 space 无关
            payload={
                "record_id": record_id,
                "title": title,
                "context": context,
                "agent_suggestion": agent_suggestion,
                "decision": decision,
                "outcome": outcome,
                "rationale": rationale,
                "decided_by": decided_by,
                "escalation_type": escalation_type,
            },
        )
        try:
            self._publish(event)
        except Exception as e:  # best-effort：留痕提交已成功，feed 失败只告警留痕
            print(
                LogEvent(
                    service="decision-log",
                    event_type="decision_feed_failed",
                    payload={"record_id": record_id, "error": str(e)},
                ).to_jsonl(),
                file=sys.stderr,
            )

    def get(self, record_id: int) -> DecisionRecord | None:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {_COLUMNS} FROM decision_log WHERE id = %s",
                (record_id,),
            )
            row = cur.fetchone()
        return DecisionRecord(*row) if row else None

    def list(self, limit: int = 50) -> list[DecisionRecord]:
        with pg_connection(self._dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {_COLUMNS} FROM decision_log ORDER BY id DESC LIMIT %s",
                (limit,),
            )
            return [DecisionRecord(*row) for row in cur.fetchall()]
