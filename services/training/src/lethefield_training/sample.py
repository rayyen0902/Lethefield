"""训练样本统一 schema（§12.4 定案，1.0 必须实现）。

`{sample_id, source, rule, space_ref, problem, diagnosis, decision, outcome,
auth_scope, review}` + created_at/scrubbed/v：
- space_ref 为不透明哈希（space_ref_of），不暴露 space_id 明文，支持成组定位；
  ①② 类运维元数据与 space 无关时为 None（清单归 `_ops` 索引）。
- problem/diagnosis/decision/outcome 为内容字段——撤回/销毁处置时整体清空，
  脱敏骨架（sample_id/source/rule/space_ref/auth_scope/review/created_at/scrubbed）保留
  （价值在"判断过程"而非"记忆内容"，§12.4）。
- review.status 未经 approved 不进正式训练集（1.0 全部 pending，标注即留痕表单）。
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

SAMPLE_VERSION = 1

# review.status 取值（1.0 只产 pending；approved/rejected 由标注流程写入）
REVIEW_STATUSES: frozenset[str] = frozenset({"pending", "approved", "rejected"})

# auth_scope：granted = ③④ 类授权 space 数据；ops_only = ①② 类运维元数据
AUTH_SCOPES: frozenset[str] = frozenset({"granted", "ops_only"})

# 撤回/销毁处置时清空的内容字段（骨架其余字段保留）
CONTENT_FIELDS: tuple[str, ...] = ("problem", "diagnosis", "decision", "outcome")


@dataclass(frozen=True)
class TrainingSample:
    sample_id: str
    source: str  # FeedSource 取值（decision_log|incident|ff_metric|ex_derived）
    rule: str  # R1|R2|R3|R5（1.0 集合）
    space_ref: str | None
    problem: dict[str, Any]
    diagnosis: dict[str, Any]
    decision: dict[str, Any]
    outcome: dict[str, Any]
    auth_scope: str
    review: dict[str, Any] = field(default_factory=lambda: {"status": "pending", "by": ""})
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    scrubbed: bool = False
    v: int = SAMPLE_VERSION

    @classmethod
    def new(
        cls,
        *,
        source: str,
        rule: str,
        space_ref: str | None,
        problem: dict[str, Any],
        diagnosis: dict[str, Any],
        decision: dict[str, Any],
        outcome: dict[str, Any],
        auth_scope: str,
    ) -> "TrainingSample":
        return cls(
            sample_id=uuid4().hex,
            source=source,
            rule=rule,
            space_ref=space_ref,
            problem=problem,
            diagnosis=diagnosis,
            decision=decision,
            outcome=outcome,
            auth_scope=auth_scope,
        )

    def scrubbed_copy(self) -> "TrainingSample":
        """内容字段清除、骨架保留（撤回/销毁等效处置，§12.4）。"""
        return TrainingSample(
            sample_id=self.sample_id,
            source=self.source,
            rule=self.rule,
            space_ref=self.space_ref,
            problem={},
            diagnosis={},
            decision={},
            outcome={},
            auth_scope=self.auth_scope,
            review=self.review,
            created_at=self.created_at,
            scrubbed=True,
        )

    def to_json(self) -> str:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        return json.dumps(data, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "TrainingSample":
        """反序列化；缺字段/版本不符/非法取值抛 ValueError（fail-closed）。"""
        obj = json.loads(data)
        if obj.get("v") != SAMPLE_VERSION:
            raise ValueError(f"样本版本不符：{obj.get('v')!r}（期望 {SAMPLE_VERSION}）")
        if obj.get("auth_scope") not in AUTH_SCOPES:
            raise ValueError(f"非法 auth_scope：{obj.get('auth_scope')!r}")
        created_at = obj["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
        return cls(
            sample_id=obj["sample_id"],
            source=obj["source"],
            rule=obj["rule"],
            space_ref=obj.get("space_ref"),
            problem=obj["problem"],
            diagnosis=obj["diagnosis"],
            decision=obj["decision"],
            outcome=obj["outcome"],
            auth_scope=obj["auth_scope"],
            review=obj.get("review", {"status": "pending", "by": ""}),
            created_at=created_at,
            scrubbed=bool(obj.get("scrubbed", False)),
        )
