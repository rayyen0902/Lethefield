"""训练数据管线 feed 信封（M11）——四入料口进训练 topic 的统一消息格式与 topic 单点。

设计依据：开发文档 §12（M11）管线架构——数据源 → 训练 topic（Pulsar 独立
tenant/namespace，与业务流 retention/配额隔离）→ 加工 worker。

约束：
- 训练 feeds 与契约 5 控制 topic 分 namespace（数据流 vs 处置命令，retention 语义不同：
  feeds 短 retention 过境、未命中明细滚动清除——"过境 ≠ 沉淀"）。
- 生产侧授权闸门（③④ 类入 topic 前拦截）在各生产路径执行，本模块只做信封与传输；
  加工 worker 侧做第二道复查。
- schema 单点定义在本模块，只能向后兼容扩展（扩展时递增 FEED_VERSION）。
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from lethefield_clients.training_control import TRAINING_TENANT

# 训练数据 feed namespace（与契约 5 控制面 namespace `control` 并列）
FEEDS_NAMESPACE = "feeds"
# 四入料口共用的原始 feed topic（单 topic + 信封 kind 区分，1.0 不按源拆 topic）
RAW_FEED_TOPIC = "raw"

FEED_VERSION = 1


class FeedKind(StrEnum):
    """feed 消息类别（判定规则的路由键）。"""

    RECALL_DETAIL = "recall_detail"  # ③ retrieve 召回明细（最小化字段，无原文）
    CORRECTION_PAIR = "correction_pair"  # ④ EX 派生纠错前后对
    DECISION_COMPARISON = "decision_comparison"  # ① 决策留痕对比（R1/R2）
    INCIDENT = "incident"  # ② 故障与混沌工程案例（人工提交）


class FeedSource(StrEnum):
    """样本 source 字段取值（与 §12.4 样本 schema 对齐）。"""

    DECISION_LOG = "decision_log"
    INCIDENT = "incident"
    FF_METRIC = "ff_metric"
    EX_DERIVED = "ex_derived"


# 各 kind 对应的样本 source（worker 路由用，单点防漂移）
KIND_SOURCE = {
    FeedKind.RECALL_DETAIL: FeedSource.FF_METRIC,
    FeedKind.CORRECTION_PAIR: FeedSource.EX_DERIVED,
    FeedKind.DECISION_COMPARISON: FeedSource.DECISION_LOG,
    FeedKind.INCIDENT: FeedSource.INCIDENT,
}

# 高价值时刻判定规则（开发文档 §12：1.0 实现 R1–R3；R5 仅人工提交触发）
RULE_R1 = "R1"  # 人类否决/修改 Agent 建议
RULE_R2 = "R2"  # §11.2 升级四类事件
RULE_R3 = "R3"  # 召回内容被纠错
RULE_R5 = "R5"  # 演练与真实事故复盘（1.0 无自动检测，显式提交即触发）

# 决策留痕 outcome 取值（M0 任务 5 定案三列之一；accepted 不产生 R1）
DECISION_OUTCOMES: frozenset[str] = frozenset({"accepted", "modified", "rejected"})
# §11.2"必须升级"四类事件
ESCALATION_TYPES: frozenset[str] = frozenset(
    {"ex_write_path", "cross_space", "novel_error", "low_confidence"}
)


def decision_rules(outcome: str, escalation_type: str | None) -> list[str]:
    """R1/R2 判定纯函数（提交路径与 worker 共用单点，开发文档 §12 v1.2 定案）。

    R1 = outcome ≠ accepted；R2 = escalation_type 非空。两规则可同时命中。
    """
    rules = []
    if outcome != "accepted":
        rules.append(RULE_R1)
    if escalation_type is not None:
        rules.append(RULE_R2)
    return rules


def feed_topic() -> str:
    """训练数据 feed topic 全限定名（生产者/worker/bootstrap 共用此单点）。"""
    return f"persistent://{TRAINING_TENANT}/{FEEDS_NAMESPACE}/{RAW_FEED_TOPIC}"


@dataclass(frozen=True)
class FeedEvent:
    """入料口 → 训练 topic 的信封。

    - kind：消息类别（worker 路由与授权复查的依据）；
    - source：样本 source（与 kind 一一对应，KIND_SOURCE 单点）；
    - space_ref：不透明哈希（space_ref_of）；①② 类运维元数据与 space 无关时为 None；
    - payload：kind 对应的明细内容（各生产路径按 kind 约定字段）。
    """

    kind: FeedKind
    source: FeedSource
    space_ref: str | None
    payload: dict[str, Any]
    emitted_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    v: int = FEED_VERSION

    def to_json(self) -> str:
        data = asdict(self)
        data["kind"] = str(self.kind)
        data["source"] = str(self.source)
        data["emitted_at"] = self.emitted_at.isoformat()
        return json.dumps(data, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "FeedEvent":
        """反序列化；缺字段/版本不符/未知 kind 抛 ValueError（fail-closed）。"""
        obj = json.loads(data)
        if obj.get("v") != FEED_VERSION:
            raise ValueError(f"feed 版本不符：{obj.get('v')!r}（期望 {FEED_VERSION}）")
        try:
            kind = FeedKind(obj["kind"])
            source = FeedSource(obj["source"])
        except (KeyError, ValueError) as e:
            raise ValueError(f"未知 feed kind/source：{obj!r}") from e
        if KIND_SOURCE[kind] is not source:
            raise ValueError(f"kind/source 不配对：{kind} vs {source}")
        emitted_at = obj["emitted_at"]
        if isinstance(emitted_at, str):
            emitted_at = datetime.fromisoformat(emitted_at)
        if emitted_at.tzinfo is None:
            emitted_at = emitted_at.replace(tzinfo=UTC)
        return cls(
            kind=kind,
            source=source,
            space_ref=obj.get("space_ref"),
            payload=obj["payload"],
            emitted_at=emitted_at,
        )


def make_feed_publisher(client) -> Callable[[FeedEvent], None]:
    """构造 feed 发布器：同步等 broker ack（失败抛异常，由调用方决定重试/告警）。

    与 destroy_broadcast 同一传输纪律；③④ 类的授权拦截在调用方（入 topic 前）。
    """
    producer = client.create_producer(feed_topic())

    def publish(event: FeedEvent) -> None:
        producer.send(event.to_json().encode("utf-8"))

    return publish
