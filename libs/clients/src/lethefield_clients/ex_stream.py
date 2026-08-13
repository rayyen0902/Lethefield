"""EX 事件流契约单点（M14，v1.2 修订记录第 20 条定案）。

EX 经验事件的 Pulsar 通道：API 摄入路径在 `append_experience` 落库确认后发布到
每 space namespace 下的 `ex-events` topic；M14 SS 消费打分后写 `scoring-results`
topic 供 M15 写入链消费。topic 命名与信封 schema 单点定义在本模块，只能向后兼容
扩展（扩展时递增 STREAM_VERSION）。

约束：
- topic 全限定名 `persistent://lethefield/{space_id}/<topic>`——tenant 固定
  `lethefield`，namespace = space_id（M9 开通流水线建 namespace 与配额）。
- 信封携带 space_id 并与 topic 名一致性校验（消费侧 fail-closed）。
- 生产侧失败不阻塞 record 同步返回（EX 是 SoT）；消费侧靠 n 连续性校验自愈
  （缺口告警 + 按 n 区间从 EX 补偿），不新立轮询组件。
"""

import json
from dataclasses import asdict, dataclass, field

from lethefield_clients.spaces import validate_space_id

BUSINESS_TENANT = "lethefield"

# M14 SS 对 ex-events 的订阅名（worker 与 DMS/巡检共用此单点）
EX_EVENTS_SUBSCRIPTION = "ss-scorer"
# M15 写入链对 scoring-results 的订阅名（M15 落地时消费；先定名单点防漂移）
SCORING_RESULTS_SUBSCRIPTION = "rms-writer"

STREAM_VERSION = 1

# 六维显著性维度键（单点，prompt/schema/指标标签共用）
DIMENSIONS: tuple[str, ...] = ("er", "e", "i", "g", "n", "c")


def ex_events_topic(space_id: str) -> str:
    """EX 经验事件流 topic 全限定名（生产侧与 SS consumer 共用此单点）。"""
    validate_space_id(space_id)
    return f"persistent://{BUSINESS_TENANT}/{space_id}/ex-events"


def scoring_results_topic(space_id: str) -> str:
    """SS 打分结果 topic 全限定名（SS 生产侧与 M15 consumer 共用此单点）。"""
    validate_space_id(space_id)
    return f"persistent://{BUSINESS_TENANT}/{space_id}/scoring-results"


def ex_events_dlq_topic(space_id: str) -> str:
    """ex-events 死信 topic 全限定名（M14 应用层死信写入与巡检共用此单点）。

    命名沿用 Pulsar 默认死信约定 `<topic>-<subscription>-DLQ`；注意死信转移由
    SS worker 应用层实现（standalone/pulsar-client 实测 broker 侧 redelivery_count
    恒 0、ConsumerDeadLetterPolicy 不触发转移，M14 踩坑记录见工作日志）。
    """
    return f"{ex_events_topic(space_id)}-{EX_EVENTS_SUBSCRIPTION}-DLQ"


def scoring_results_dlq_topic(space_id: str) -> str:
    """scoring-results 死信 topic 全限定名（M15 写入链应用层死信单点）。

    命名同款 `<topic>-<subscription>-DLQ`；死信转移同样由 writer worker 应用层
    实现（broker 侧策略在本栈不生效，见 ex_events_dlq_topic 注释）。
    """
    return f"{scoring_results_topic(space_id)}-{SCORING_RESULTS_SUBSCRIPTION}-DLQ"


def space_id_of_topic(topic: str) -> str:
    """从 topic 全限定名解析 space_id（namespace 段）；形式不符抛 ValueError。"""
    parts = topic.split("/")
    # persistent://lethefield/{space_id}/ex-events → ["persistent:", "", tenant, ns, name]
    if len(parts) != 5 or parts[2] != BUSINESS_TENANT:
        raise ValueError(f"topic 形式不符（无法解析 space_id）：{topic!r}")
    return validate_space_id(parts[3])


@dataclass(frozen=True)
class ExStreamEvent:
    """EX 摄入路径 → ex-events topic 的信封（经验事件全字段快照）。"""

    space_id: str
    event_id: str
    n: int
    content: str
    agent_actor_id: str | None
    account_id: str | None
    tau_ms: int | None
    ref_conflict: str | None
    created_at_ms: int
    v: int = STREAM_VERSION

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "ExStreamEvent":
        """反序列化；缺字段/版本不符抛 ValueError（fail-closed）。"""
        obj = json.loads(data)
        if obj.get("v") != STREAM_VERSION:
            raise ValueError(f"ex-events 信封版本不符：{obj.get('v')!r}（期望 {STREAM_VERSION}）")
        try:
            return cls(
                space_id=obj["space_id"],
                event_id=obj["event_id"],
                n=obj["n"],
                content=obj["content"],
                agent_actor_id=obj["agent_actor_id"],
                account_id=obj["account_id"],
                tau_ms=obj["tau_ms"],
                ref_conflict=obj["ref_conflict"],
                created_at_ms=obj["created_at_ms"],
            )
        except KeyError as e:
            raise ValueError(f"ex-events 信封缺字段 {e}：{obj!r}") from e


@dataclass(frozen=True)
class ScoringResult:
    """SS → scoring-results topic 的信封（六维原始值与合成 s 分开存储，权重可后调）。

    degraded/missing_dims：M14 降级规则定案——缺 1 维置中性值并标记，随结果
    一路落 EX（未来模型升级/权重标定后可识别、可重打分）。
    """

    space_id: str
    event_id: str
    n: int
    node_key: str
    dims: dict[str, float]
    s: float
    model_version: str
    degraded: bool
    missing_dims: list[str] = field(default_factory=list)
    scored_at_ms: int = 0
    v: int = STREAM_VERSION

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "ScoringResult":
        """反序列化；缺字段/版本不符/维度键异常抛 ValueError（fail-closed）。"""
        obj = json.loads(data)
        if obj.get("v") != STREAM_VERSION:
            raise ValueError(f"scoring 信封版本不符：{obj.get('v')!r}（期望 {STREAM_VERSION}）")
        try:
            dims = obj["dims"]
            if set(dims) != set(DIMENSIONS):
                raise ValueError(f"维度键不符：{sorted(dims)!r}（期望 {sorted(DIMENSIONS)}）")
            return cls(
                space_id=obj["space_id"],
                event_id=obj["event_id"],
                n=obj["n"],
                node_key=obj["node_key"],
                dims={k: float(v) for k, v in dims.items()},
                s=float(obj["s"]),
                model_version=obj["model_version"],
                degraded=bool(obj["degraded"]),
                missing_dims=list(obj.get("missing_dims") or []),
                scored_at_ms=int(obj.get("scored_at_ms") or 0),
            )
        except KeyError as e:
            raise ValueError(f"scoring 信封缺字段 {e}：{obj!r}") from e
