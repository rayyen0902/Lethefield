"""加工 worker（M11）：训练 feed topic → 样本封装/授权复查/热层落盘；契约 5 销毁处置。

红线 1 结构保证：本模块只消费 topic + 读写本地热层 + 查授权注册表（Postgres
元数据，非业务库），无任何 RMS/EX/图 访问路径（静态测试强制）。

双 consumer 单进程轮询：
- feed topic（`training-sample-worker` durable 订阅）：四入料口消息；
- 控制 topic（`training-destroy-sink`，复用 M10 订阅名继承积压）：销毁指令处置
  = scrub 存量样本 + 删注册表项 + 处置动作进决策留痕（LogEvent）。

消息语义同 M10 sink：schema 不符/处置失败不 ack，留 broker 重投（不静默吞）。
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

import pulsar
from lethefield_clients import (
    DESTROY_SUBSCRIPTION,
    RULE_R3,
    RULE_R5,
    AuthRegistryStore,
    AuthScope,
    FeedEvent,
    FeedKind,
    SpaceDestroyCommand,
    control_topic,
    decision_rules,
    feed_topic,
)
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_log
from lethefield_metrics import counter
from prometheus_client import REGISTRY
from pulsar import Client

from lethefield_training.config import DEFAULT_CONFIG, TrainingConfig
from lethefield_training.hot_store import HotSampleStore
from lethefield_training.recall_window import RecallWindow
from lethefield_training.sample import TrainingSample

# feed topic 的 durable 订阅名
FEED_SUBSCRIPTION = "training-sample-worker"

# ③④ 类 feed 的 worker 侧授权复查（第二道防线；第一道在生产侧入 topic 前）
AUTH_BY_KIND = {
    FeedKind.RECALL_DETAIL: AuthScope.CALIBRATION,
    FeedKind.CORRECTION_PAIR: AuthScope.CONTENT_COPY,
}

SAMPLES_TOTAL = counter(
    "lethefield_training_samples_total",
    "训练样本产出计数（§12.4 课题定案指标）",
    ["source", "rule", "review_status"],
    registry=REGISTRY,
)
FEED_DROPPED_TOTAL = counter(
    "lethefield_training_feed_dropped_total",
    "训练 feed 丢弃计数（未授权/规则未命中）",
    ["kind", "reason"],
    registry=REGISTRY,
)


@dataclass
class WorkerDeps:
    """worker 依赖容器（构造时注入，测试逐项换 fake）。"""

    store: HotSampleStore
    window: RecallWindow
    registry: AuthRegistryStore
    emit: Callable[[LogEvent], None]
    config: TrainingConfig = DEFAULT_CONFIG


def _emit_drop(deps: WorkerDeps, event: FeedEvent, reason: str) -> None:
    FEED_DROPPED_TOTAL.labels(kind=str(event.kind), reason=reason).inc()
    deps.emit(
        LogEvent(
            service="lethefield-training",
            event_type="training_feed_dropped",
            payload={"kind": str(event.kind), "reason": reason, "space_ref": event.space_ref},
        )
    )


def _samples_from_decision(event: FeedEvent) -> list[TrainingSample]:
    """① 决策留痕对比 → R1/R2 样本（两规则可同中，各产一条；判定单点 decision_rules）。"""
    p = event.payload
    rules = decision_rules(p["outcome"], p.get("escalation_type"))
    return [
        TrainingSample.new(
            source=str(event.source),
            rule=rule,
            space_ref=None,
            problem={"title": p["title"], "context": p.get("context", "")},
            diagnosis={"agent_suggestion": p.get("agent_suggestion", "")},
            decision={"decision": p["decision"], "decided_by": p["decided_by"]},
            outcome={
                "outcome": p["outcome"],
                "rationale": p.get("rationale", ""),
                "escalation_type": p.get("escalation_type"),
                "record_id": p["record_id"],
            },
            auth_scope="ops_only",
        )
        for rule in rules
    ]


def _sample_from_incident(event: FeedEvent) -> TrainingSample:
    """② 故障/混沌案例（人工提交即触发，rule=R5；无自动检测，R5 自动规则仍属延后）。"""
    p = event.payload
    return TrainingSample.new(
        source=str(event.source),
        rule=RULE_R5,
        space_ref=event.space_ref,
        problem=p.get("problem", {}),
        diagnosis=p.get("diagnosis", {}),
        decision=p.get("decision", {}),
        outcome=p.get("outcome", {}),
        auth_scope="ops_only",
    )


def _sample_from_correction(event: FeedEvent, deps: WorkerDeps) -> TrainingSample | None:
    """④ 纠错对 × 召回明细窗 → R3 样本；未命中（未经召回的纠错）不计入 R3（语义边界）。"""
    p = event.payload
    recalled_at = deps.window.recalled_at(event.space_ref or "", p["old_node_key"])
    if recalled_at is None:
        return None
    return TrainingSample.new(
        source=str(event.source),
        rule=RULE_R3,
        space_ref=event.space_ref,
        problem={"recalled_node_key": p["old_node_key"], "recalled_at_ms": recalled_at},
        diagnosis={"before": p["before"], "after": p["after"]},
        decision={"correction_node_key": p["new_node_key"]},
        outcome={"corrected_at": p["corrected_at"]},
        auth_scope="granted",
    )


def process_feed_event(event: FeedEvent, deps: WorkerDeps) -> None:
    """单条 feed 消息：授权复查 → 路由 → 落盘。schema/字段缺失抛异常（不 ack）。"""
    scope = AUTH_BY_KIND.get(event.kind)
    if scope is not None:
        if event.space_ref is None:
            raise ValueError(f"{event.kind} 缺 space_ref（③④ 类必带，fail-closed）")
        if not deps.registry.is_authorized(event.space_ref, scope):
            _emit_drop(deps, event, "unauthorized")
            return

    samples: list[TrainingSample] = []
    if event.kind is FeedKind.RECALL_DETAIL:
        event_id = event.payload.get("event_id")
        if not event_id:
            raise ValueError("recall_detail 缺 event_id（M12 收口定案去重键，fail-closed）")
        recalled_at_ms = int(event.emitted_at.timestamp() * 1000)
        if not deps.window.mark_seen(event_id, at_ms=recalled_at_ms):
            # at-least-once 重放：去重跳过（只计数，不发事件——重放可以是常态噪声）
            FEED_DROPPED_TOTAL.labels(kind=str(event.kind), reason="duplicate").inc()
            return
        deps.window.record(
            event.space_ref or "",
            event.payload["node_keys"],
            recalled_at_ms=recalled_at_ms,
        )
    elif event.kind is FeedKind.CORRECTION_PAIR:
        sample = _sample_from_correction(event, deps)
        if sample is None:
            _emit_drop(deps, event, "r3_miss")  # 未经召回的纠错不计入 R3
            return
        samples = [sample]
    elif event.kind is FeedKind.DECISION_COMPARISON:
        samples = _samples_from_decision(event)
        if not samples:  # accepted 且无升级类型 = 常规流量，不产样本（既定）
            _emit_drop(deps, event, "no_rule")
            return
    elif event.kind is FeedKind.INCIDENT:
        samples = [_sample_from_incident(event)]

    deps.store.append(samples)
    for s in samples:
        SAMPLES_TOTAL.labels(source=s.source, rule=s.rule, review_status=s.review["status"]).inc()


def process_control_message(data: bytes, deps: WorkerDeps) -> None:
    """契约 5 销毁指令处置：scrub 存量样本（O(清单)）→ 删注册表项 → 处置留痕。"""
    command = SpaceDestroyCommand.from_json(data.decode("utf-8"))
    scrubbed = deps.store.scrub(command.space_ref)
    deleted = deps.registry.delete(command.space_ref)
    deps.emit(
        LogEvent(
            service="lethefield-training",
            event_type="training_space_destroy_processed",
            payload={
                "space_ref": command.space_ref,
                "scrubbed_count": scrubbed,
                "registry_entry_deleted": deleted,
                "initiator": command.initiator,
                "ticket_ref": command.ticket_ref,
                "command_timestamp": command.timestamp.isoformat(),
            },
        )
    )


def _subscribe_with_retry(
    client: Client, topic: str, subscription: str, *, timeout_s: float = 10.0
):
    """订阅（durable）。Exclusive 订阅在前一 consumer 关闭后短暂窗口内会 ConsumerBusy
    （broker 侧连接回收滞后），退避重试；其他异常上抛。"""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return client.subscribe(topic, subscription)
        except pulsar.ConsumerBusy:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)


class WorkerRuntime:
    """常驻 consumer 对（feed + 控制）：生产形态订阅常开，不按轮重建。"""

    def __init__(self, client: Client, deps: WorkerDeps) -> None:
        self._deps = deps
        self._feed = _subscribe_with_retry(client, feed_topic(), FEED_SUBSCRIPTION)
        self._control = _subscribe_with_retry(client, control_topic(), DESTROY_SUBSCRIPTION)

    def run_once(self, *, timeout_ms: int | None = None) -> int:
        """单轮：排空两个 topic 当前可达消息并逐条处理（返回处理条数）。"""
        deps = self._deps
        timeout_ms = timeout_ms if timeout_ms is not None else deps.config.receive_timeout_ms
        processed = 0
        while True:
            progressed = False
            for consumer, handler in (
                (self._feed, lambda data: process_feed_event(FeedEvent.from_json(data), deps)),
                (self._control, lambda data: process_control_message(data, deps)),
            ):
                try:
                    msg = consumer.receive(timeout_millis=timeout_ms)
                except pulsar.Timeout:  # 超时即当前无更多消息（其他异常不静默上抛）
                    continue
                handler(msg.data())
                consumer.acknowledge(msg)
                processed += 1
                progressed = True
            if not progressed:
                return processed

    def close(self) -> None:
        self._feed.close()
        self._control.close()


def run_once(client: Client, deps: WorkerDeps, *, timeout_ms: int | None = None) -> int:
    """单轮便利封装（测试/巡检用）；常驻形态用 WorkerRuntime + run_forever。"""
    runtime = WorkerRuntime(client, deps)
    try:
        return runtime.run_once(timeout_ms=timeout_ms)
    finally:
        runtime.close()


def run_forever(client: Client, deps: WorkerDeps, *, idle_sleep: float = 1.0) -> None:
    """常驻循环（失败上抛由进程监督重启；畸形消息不 ack 留重投——同 M10 sink 语义）。"""
    runtime = WorkerRuntime(client, deps)
    try:
        while True:
            runtime.run_once()
            time.sleep(idle_sleep)
    finally:
        runtime.close()


def default_emit(event: LogEvent) -> None:
    emit_log(event)
