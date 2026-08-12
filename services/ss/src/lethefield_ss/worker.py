"""SS 打分 worker（M14）：ex-events consumer → 六维打分 → EX 回写 + scoring-results 发布。

处理流（v1.2 定案）：
- 逐 space consumer（ControlPlaneStore.list_spaces() 枚举 + 节流刷新；Pulsar 跨
  namespace 正则订阅实测 InvalidTopicName，namespace 段不许通配），订阅 `ss-scorer`。
- n 连续性自愈（修订记录第 20 条）：per-space last_n 追踪（冷启动从 EX 最新
  scoring_result 的 n_at_event 播种），缺口 → page 告警 + 按 n 区间从 EX 补偿打分。
- EX 回写幂等：node_key 已有 scoring_result 则跳过 LLM 重打分（成本与确定性双赢），
  从 details 重建信封补发下游；重复投递/重放安全（M15 侧 ref_ex 幂等已定案）。
- 失败路径（应用层死信）：打分/回写/发布失败 → nack 重投；runtime 按 message_id
  计失败次数，超 `max_redeliver_count` → 原文写死信 topic（`ex_stream.
  ex_events_dlq_topic` 单点）+ ack 原消息 + page 级 `ss_scoring_dlq`——禁静默丢弃。
  不走 broker ConsumerDeadLetterPolicy：standalone + pulsar-client 实测
  redelivery_count 恒 0、broker 不触发死信转移（M14 探针实测）。
"""

import time
from collections.abc import Callable
from datetime import UTC

import pulsar
from lethefield_clients.ex_n import append_meta_row, list_experience_events, list_meta_events
from lethefield_clients.ex_stream import (
    EX_EVENTS_SUBSCRIPTION,
    ExStreamEvent,
    ScoringResult,
    ex_events_dlq_topic,
    ex_events_topic,
    scoring_results_topic,
    space_id_of_topic,
)
from lethefield_clients.redline import redline1_exempt
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_log
from lethefield_metrics import counter, histogram
from lethefield_rms.rebuild import node_key_of
from lethefield_rms.schema import (
    SCORING_RESULT_META_TYPE,
    parse_scoring_details,
    scoring_details_of,
)
from prometheus_client import REGISTRY
from pulsar import Client

from lethefield_ss.config import DEFAULT_CONFIG, SSConfig
from lethefield_ss.scoring import score_event

DEFAULT_METRICS_PORT = 9105

# 标定线：六维分值与合成 s 分布（§19.2 标定输入；dimension=er|e|i|g|n|c|s）
SCORE_RATIO = histogram(
    "lethefield_ss_score_ratio",
    "SS 六维分值与合成 s 分布（标定线）",
    ["dimension"],
    registry=REGISTRY,
)
SCORE_DEGRADED_TOTAL = counter(
    "lethefield_ss_score_degraded_total",
    "降级打分计数（缺 1 维置中性值并标记）",
    ["dimension"],
    registry=REGISTRY,
)
SCORING_DURATION = histogram(
    "lethefield_ss_scoring_duration_seconds",
    "单事件 LLM 打分耗时",
    registry=REGISTRY,
)
LLM_CALLS_TOTAL = counter(
    "lethefield_ss_llm_calls_total",
    "LLM 打分调用计数（标定线：调用量）",
    ["result"],
    registry=REGISTRY,
)
LLM_TOKENS_TOTAL = counter(
    "lethefield_ss_llm_tokens_total",
    "LLM token 用量（标定线：成本曲线输入）",
    ["type"],
    registry=REGISTRY,
)
DLQ_TOTAL = counter(
    "lethefield_ss_dlq_total",
    "重试耗尽转死信的打分单计数",
    registry=REGISTRY,
)
N_GAP_TOTAL = counter(
    "lethefield_ss_n_gap_total",
    "ex-events 消费侧 n 连续性缺口计数（发布失败的自愈触发点）",
    registry=REGISTRY,
)


class ResultPublisher:
    """scoring-results 发布器：per-space producer 懒建缓存，同步等 broker ack。"""

    def __init__(self, client: Client) -> None:
        self._client = client
        self._producers: dict[str, object] = {}

    def publish(self, result: ScoringResult) -> None:
        space_id = result.space_id
        producer = self._producers.get(space_id)
        if producer is None:
            producer = self._client.create_producer(scoring_results_topic(space_id))
            self._producers[space_id] = producer
        producer.send(result.to_json().encode("utf-8"))

    def close(self) -> None:
        for producer in self._producers.values():
            producer.close()
        self._producers.clear()


class NTracker:
    """per-space n 连续性追踪（消费侧自愈，v1.2 修订记录第 20 条）。

    冷启动从 EX 最新 scoring_result 的 n_at_event 播种——重启后仍能检出
    "EX 落库成功但发布失败/进程崩溃"留下的缺口。space 内扫描不违红线 1。
    """

    def __init__(self, ex_session) -> None:
        self._session = ex_session
        self._last: dict[str, int] = {}

    def check(self, event: ExStreamEvent) -> range | None:
        """返回缺口 n 区间（空 range 视 None）；无缺口返回 None。"""
        last = self._last.get(event.space_id)
        if last is None:
            last = self._seed(event.space_id)
            self._last[event.space_id] = last
        if event.n > last + 1:
            return range(last + 1, event.n)
        return None

    def mark(self, space_id: str, n: int) -> None:
        self._last[space_id] = max(self._last.get(space_id, 0), n)

    def _seed(self, space_id: str) -> int:
        metas = list_meta_events(self._session, space_id=space_id)
        scored = [
            m.n_at_event
            for m in metas
            if m.meta_type == SCORING_RESULT_META_TYPE and m.n_at_event is not None
        ]
        return max(scored, default=0)


class WorkerDeps:
    """worker 依赖容器（构造时注入，测试逐项换 fake）。

    scorer 协议：`score(content) -> (raw_text, usage, model)`（LLMScorer 同款）。
    publisher 协议：`publish(ScoringResult)`（ResultPublisher 同款）。
    control_store：ControlPlaneStore 抽象（list_spaces() 枚举起订集合，红线 1 要件①）。
    """

    def __init__(
        self,
        *,
        scorer,
        ex_session,
        publisher,
        control_store,
        emit: Callable[[LogEvent], None],
        config: SSConfig = DEFAULT_CONFIG,
    ) -> None:
        self.scorer = scorer
        self.ex_session = ex_session
        self.publisher = publisher
        self.control_store = control_store
        self.emit = emit
        self.config = config


def _ex_event_to_stream(ex_event, space_id: str) -> ExStreamEvent:
    """EX 行 → 流信封（补偿路径用；created_at 转 epoch 毫秒，naive 按 UTC，M6 踩坑定案）。"""
    created = ex_event.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    return ExStreamEvent(
        space_id=space_id,
        event_id=ex_event.event_id,
        n=ex_event.n,
        content=ex_event.content,
        agent_actor_id=ex_event.agent_actor_id,
        account_id=ex_event.account_id,
        tau_ms=ex_event.tau_ms,
        ref_conflict=ex_event.ref_conflict,
        created_at_ms=int(created.timestamp() * 1000),
    )


def _score_and_record(event: ExStreamEvent, deps: WorkerDeps) -> None:
    """打分 + EX 回写 + 下游发布（消费/补偿共用的单事件处理）。

    EX 回写幂等：已有 scoring_result → 不重打分（LLM 不可重放且计费），
    从 details 重建信封补发下游——覆盖"EX 写成功但发布前崩溃"的 redelivery 场景。
    """
    node_key = node_key_of(event.event_id)
    existing = [
        m
        for m in list_meta_events(deps.ex_session, space_id=event.space_id, node_key=node_key)
        if m.meta_type == SCORING_RESULT_META_TYPE and m.details
    ]
    if existing:
        details = parse_scoring_details(existing[-1].details)  # created_at 升序，取最新
        result = ScoringResult(
            space_id=event.space_id,
            event_id=event.event_id,
            n=event.n,
            node_key=node_key,
            dims=details.dims,
            s=details.s,
            model_version=details.model_version,
            degraded=details.degraded,
            missing_dims=details.missing_dims,
        )
    else:
        t0 = time.perf_counter()
        try:
            result, usage = score_event(event, scorer=deps.scorer, config=deps.config)
        except Exception:
            LLM_CALLS_TOTAL.labels(result="failed").inc()
            raise
        SCORING_DURATION.observe(time.perf_counter() - t0)
        LLM_CALLS_TOTAL.labels(result="ok").inc()
        LLM_TOKENS_TOTAL.labels(type="prompt").inc(usage.get("prompt_tokens", 0))
        LLM_TOKENS_TOTAL.labels(type="completion").inc(usage.get("completion_tokens", 0))
        for dim, value in result.dims.items():
            SCORE_RATIO.labels(dimension=dim).observe(value)
        SCORE_RATIO.labels(dimension="s").observe(result.s)
        if result.degraded:
            SCORE_DEGRADED_TOTAL.labels(dimension=result.missing_dims[0]).inc()
        append_meta_row(
            deps.ex_session,
            space_id=event.space_id,
            node_key=node_key,
            meta_type=SCORING_RESULT_META_TYPE,
            n_at_event=event.n,
            agent_actor_id=event.agent_actor_id,
            account_id=event.account_id,
            details=scoring_details_of(
                dims=result.dims,
                s=result.s,
                model_version=result.model_version,
                event_id=result.event_id,
                degraded=result.degraded,
                missing_dims=result.missing_dims,
            ),
        )
    deps.publisher.publish(result)


def process_message(msg, deps: WorkerDeps, tracker: NTracker) -> None:
    """单条 ex-events 消息：信封校验 → n 连续性 → 打分回写发布。异常上抛（由运行时 nack）。"""
    event = ExStreamEvent.from_json(msg.data().decode("utf-8"))
    topic_space = space_id_of_topic(msg.topic_name())
    if topic_space != event.space_id:
        raise ValueError(
            f"信封 space_id 与 topic 不符：{event.space_id!r} vs {topic_space!r}（fail-closed）"
        )
    gap = tracker.check(event)
    if gap:
        N_GAP_TOTAL.inc()
        deps.emit(
            LogEvent(
                service="lethefield-ss",
                event_type="ss_n_gap",
                space_id=event.space_id,
                payload={
                    "from_n": gap.start,
                    "to_n": gap.stop - 1,
                    "message": "ex-events n 连续性缺口（EX 落库成功但发布失败/崩溃），"
                    "按 n 区间从 EX 补偿打分",
                },
            )
        )
        wanted = set(gap)
        for ex_event in list_experience_events(deps.ex_session, space_id=event.space_id):
            if ex_event.n in wanted:
                _score_and_record(_ex_event_to_stream(ex_event, event.space_id), deps)
                tracker.mark(event.space_id, ex_event.n)
    _score_and_record(event, deps)
    tracker.mark(event.space_id, event.n)


def _subscribe_with_retry(
    client: Client, space_id: str, config: SSConfig, *, timeout_s: float = 10.0
):
    """订阅单 space 的 ex-events（durable）。ConsumerBusy 退避重试
    （Exclusive 订阅连接回收滞后，M11 踩坑定案）；其他异常上抛。"""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return client.subscribe(
                ex_events_topic(space_id),
                EX_EVENTS_SUBSCRIPTION,
                negative_ack_redelivery_delay_ms=config.nack_redelivery_delay_ms,
                # 新订阅从最早位点起消费：worker 部署/重启晚于发布时不能漏 backlog
                initial_position=pulsar.InitialPosition.Earliest,
            )
        except pulsar.ConsumerBusy:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)


class WorkerRuntime:
    """常驻 consumer 组：每 active space 一个 consumer，订阅常开不按轮重建。

    space 发现（红线 1 合规形态）：经 ControlPlaneStore.list_spaces() 枚举、按
    topic_discovery_seconds 节流刷新——Pulsar 跨 namespace 正则订阅实测被拒
    （InvalidTopicName，namespace 段不允许通配），逐 space 订阅与 FS sweep 同款。
    """

    def __init__(self, client: Client, deps: WorkerDeps) -> None:
        self._client = client
        self._deps = deps
        self._consumers: dict[str, object] = {}
        self._dlq_producers: dict[str, object] = {}
        self._fail_counts: dict[str, int] = {}  # message_id → 连续失败次数（应用层死信计数）
        self._failed_spaces: set[str] = set()  # 订阅失败的 space（恢复时出列）
        self._last_refresh = 0.0
        self._tracker = NTracker(deps.ex_session)
        self._refresh_consumers()

    def _refresh_consumers(self) -> None:
        now = time.monotonic()
        if self._consumers and now - self._last_refresh < self._deps.config.topic_discovery_seconds:
            return
        self._last_refresh = now
        active = set(self._deps.control_store.list_spaces())
        for space_id in active - self._consumers.keys():
            try:
                self._consumers[space_id] = _subscribe_with_retry(
                    self._client, space_id, self._deps.config
                )
            except Exception as exc:
                # namespace/topic 未就绪（开通中途、注销竞态、存储直注册无 namespace 的
                # 特殊形态）：跳过本轮、下轮重试；状态翻转时 observation 日志（不 page，
                # 开通/注销窗口属常态）。
                if space_id not in self._failed_spaces:
                    self._deps.emit(
                        LogEvent(
                            service="lethefield-ss",
                            event_type="ss_space_subscribe_failed",
                            space_id=space_id,
                            payload={"error": f"{type(exc).__name__}: {exc}"},
                        )
                    )
                    self._failed_spaces.add(space_id)
            else:
                self._failed_spaces.discard(space_id)
        for space_id in set(self._consumers) - active:  # 注销的 space 撤订
            self._consumers.pop(space_id).close()

    @redline1_exempt(
        worker="ss-scorer",
        reason=(
            "枚举走 ControlPlaneStore.list_spaces()（映射表 active 集合）；逐 space 独立 "
            "consumer/独立处理单元（无跨 space 联合查询，n 补偿限定单 keyspace）；"
            "批间节流 = topic_discovery_seconds 刷新间隔 + receive 空轮 timeout"
        ),
        cadence="Pulsar 推送节奏；space 列表刷新节流默认 5s；run_forever 轮间 sleep 1s",
    )
    def run_once(self, *, timeout_ms: int | None = None) -> int:
        """单轮：排空全部 space consumer 当前可达消息并逐条处理（返回处理条数）。

        处理失败：按 message_id 计连续失败次数，未超上限 → nack 留重投（失败不计
        progressed——毒消息重投会把"排空"循环喂成死循环，实测踩坑）；超上限 →
        应用层死信（原文写 DLQ topic + ack + page 级 ss_scoring_dlq）。
        """
        self._refresh_consumers()
        timeout_ms = timeout_ms if timeout_ms is not None else self._deps.config.receive_timeout_ms
        processed = 0
        while True:
            progressed = False
            for space_id, consumer in list(self._consumers.items()):
                try:
                    msg = consumer.receive(timeout_millis=timeout_ms)
                except pulsar.Timeout:  # 超时即当前无更多消息（其他异常不静默上抛）
                    continue
                try:
                    process_message(msg, self._deps, self._tracker)
                except Exception as exc:
                    self._handle_failure(space_id, consumer, msg, exc)
                    continue
                self._fail_counts.pop(str(msg.message_id()), None)
                consumer.acknowledge(msg)
                processed += 1
                progressed = True
            if not progressed:
                return processed

    def _handle_failure(self, space_id: str, consumer, msg, exc: Exception) -> None:
        """失败分级：未超重试上限 → nack 重投；超上限 → 应用层死信转移。"""
        message_id = str(msg.message_id())
        fails = self._fail_counts.get(message_id, 0) + 1
        self._fail_counts[message_id] = fails
        if fails <= self._deps.config.max_redeliver_count:
            consumer.negative_acknowledge(msg)
            return
        # 重试耗尽：原文转死信 topic（不丢单）+ ack 原消息（不重单）+ page 告警
        producer = self._dlq_producers.get(space_id)
        if producer is None:
            producer = self._client.create_producer(ex_events_dlq_topic(space_id))
            self._dlq_producers[space_id] = producer
        producer.send(msg.data())
        consumer.acknowledge(msg)
        self._fail_counts.pop(message_id, None)
        DLQ_TOTAL.inc()
        self._deps.emit(
            LogEvent(
                service="lethefield-ss",
                event_type="ss_scoring_dlq",
                space_id=space_id,
                payload={
                    "topic": msg.topic_name(),
                    "failures": fails,
                    "error": f"{type(exc).__name__}: {exc}",
                    "message": "打分重试耗尽，消息已转死信 topic",
                },
            )
        )

    def close(self) -> None:
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()
        for producer in self._dlq_producers.values():
            producer.close()
        self._dlq_producers.clear()


def run_once(client: Client, deps: WorkerDeps, *, timeout_ms: int | None = None) -> int:
    """单轮便利封装（测试/巡检用）；常驻形态用 WorkerRuntime + run_forever。"""
    runtime = WorkerRuntime(client, deps)
    try:
        return runtime.run_once(timeout_ms=timeout_ms)
    finally:
        runtime.close()


def run_forever(client: Client, deps: WorkerDeps, *, idle_sleep: float = 1.0) -> None:
    """常驻循环（失败上抛由进程监督重启；失败消息 nack 留重投 → DLQ）。"""
    runtime = WorkerRuntime(client, deps)
    try:
        while True:
            runtime.run_once()
            time.sleep(idle_sleep)
    finally:
        runtime.close()


def default_emit(event: LogEvent) -> None:
    emit_log(event)
