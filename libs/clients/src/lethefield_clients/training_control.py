"""训练管线控制面契约（契约 5，M10 冻结最小字段集）。

设计依据：开发文档 M10/M11 + 任务划分契约 5——space 注销流水线第 4 步
"向训练管线广播销毁指令"的真实接口 = 训练 tenant 持久化控制 topic。

三条硬性约束（定案，缺一不算完成）：
1. 生产者等 broker ack；最终失败 → 注销第 4 步标记失败 + 告警 + 留痕，
   禁止静默进入第 5 步（销毁指令是义务级，不是 fire-and-forget）。
2. 控制 topic 的 consumer backlog 纳入监控告警（ops/ingest_dms）——
   consumer 停摆 = 销毁指令静默积压 = 合规失效。
3. 指令消息 schema 单点定义在本模块（与契约 1 同处管理的约定）；
   M11 加工 worker 消费同一 topic，只能向后兼容扩展本 schema。

tenant/namespace 与业务流隔离、retention/配额独立（M11 管线架构既定原则）。
"""

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

# 训练管线独立 tenant（与业务流 tenant `lethefield` 隔离，retention/配额独立配置）
TRAINING_TENANT = "lethefield-training"
# 控制面 namespace（销毁指令等异步处置命令；与训练数据 topic 分开）
CONTROL_NAMESPACE = "control"
# 整 space 销毁指令 topic（persistent）
DESTROY_TOPIC = "space-destroy"

# 契约 5 schema 版本（M11 只能向后兼容扩展，扩展时递增）
CONTRACT_VERSION = 1

COMMAND_SPACE_DESTROY = "space_destroy"

# durable subscription 名（契约 5 消费侧单点）：M10 sink 与 M11 加工 worker 共用——
# 复用同名订阅继承 consumer 离线期间的 broker 侧积压。
DESTROY_SUBSCRIPTION = "training-destroy-sink"


def control_topic() -> str:
    """销毁指令 topic 全限定名（生产者/consumer/DMS backlog 监控共用此单点）。"""
    return f"persistent://{TRAINING_TENANT}/{CONTROL_NAMESPACE}/{DESTROY_TOPIC}"


def space_ref_of(space_id: str) -> str:
    """space_id → 不透明 space_ref 哈希（契约 5 / M11 样本 schema 共用单点）。

    不暴露 space_id 明文，支持成组定位（同一 space 的样本/指令可按 ref 聚齐）。
    域分隔前缀防跨语境哈希碰撞复用。
    """
    return hashlib.sha256(f"lethefield:space:{space_id}".encode()).hexdigest()


@dataclass(frozen=True)
class SpaceDestroyCommand:
    """整 space 销毁指令（契约 5 冻结最小字段集）。

    - space_ref：不透明哈希（space_ref_of），不含 space_id 明文；
    - initiator：指令发起者（注销流程操作者/系统账号标识）；
    - ticket_ref：注销工单引用（决策留痕工单号，审计链回溯用）；
    - timestamp：指令发出时间（UTC）。
    """

    space_ref: str
    initiator: str
    ticket_ref: str
    command_type: str = COMMAND_SPACE_DESTROY
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    v: int = CONTRACT_VERSION

    def to_json(self) -> str:
        payload = asdict(self)
        payload["timestamp"] = self.timestamp.isoformat()
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "SpaceDestroyCommand":
        """反序列化；缺字段/版本不符抛 ValueError（fail-closed，不静默吞指令）。"""
        obj = json.loads(data)
        if obj.get("v") != CONTRACT_VERSION:
            raise ValueError(f"契约 5 版本不符：{obj.get('v')!r}（期望 {CONTRACT_VERSION}）")
        if obj.get("command_type") != COMMAND_SPACE_DESTROY:
            raise ValueError(f"未知指令类型：{obj.get('command_type')!r}")
        ts = obj["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return cls(
            space_ref=obj["space_ref"],
            initiator=obj["initiator"],
            ticket_ref=obj["ticket_ref"],
            timestamp=ts,
        )
