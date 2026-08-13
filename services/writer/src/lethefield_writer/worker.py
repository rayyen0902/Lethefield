"""写入链 worker（M15）：scoring-results consumer → RMS 图顶点 + 时序边 + 向量。

处理流（v1.2 修订记录第 23 条定案）：
- 逐 space consumer（ControlPlaneStore.list_spaces() 枚举 + 节流刷新；Pulsar 跨
  namespace 正则订阅实测 InvalidTopicName，namespace 段不许通配），订阅 `rms-writer`。
- 单消息处理：信封校验（版本/space 与 topic 一致/node_key 与 event_id 一致，
  fail-closed）→ n 连续性 → ensure_node 幂等建点（nodes.py 单点）。
- n 连续性自愈（第 23 条⑤）：per-space last_n 追踪（冷启动从图内 max n_created
  播种——writer 自己的产出口径，M7 重建后同样正确），缺口 → page 告警 +
  按 n 区间从 EX 补偿建点（s 取 EX scoring_result details 全保真档；details
  缺失 = SS 尚未打分 → 跳过等 SS 侧补偿重发，正常路径幂等兜底不重复建点）。
- 元事件（reinforce 等）不经 stream_publisher、不进 scoring-results——"不建经验
  顶点、不推进 n_now"由链路拓扑保证（开发文档 §16 验收项）。
- 失败路径（应用层死信）：建点/嵌入失败 → nack 重投；runtime 按 message_id 计
  失败次数，超 `max_redeliver_count` → 原文写死信 topic（`ex_stream.
  scoring_results_dlq_topic` 单点）+ ack 原消息 + page 级 `writer_dlq`——禁静默
  丢弃。不走 broker ConsumerDeadLetterPolicy：standalone + pulsar-client 实测
  redelivery_count 恒 0、broker 不触发死信转移（M14 探针实测）。
"""

import time
from collections.abc import Callable

import pulsar
from lethefield_clients.ex_n import list_experience_events_range, list_meta_events
from lethefield_clients.ex_stream import (
    SCORING_RESULTS_SUBSCRIPTION,
    ScoringResult,
    scoring_results_dlq_topic,
    scoring_results_topic,
    space_id_of_topic,
)
from lethefield_clients.redline import redline1_exempt
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_log
from lethefield_rms import writer as rms_writer
from lethefield_rms.rebuild import node_key_of
from lethefield_rms.schema import SCORING_RESULT_META_TYPE, parse_scoring_details
from pulsar import Client

from lethefield_writer.config import DEFAULT_CONFIG, WriterConfig
from lethefield_writer.metrics import DLQ_TOTAL, N_GAP_TOTAL, NODE_WRITE_TOTAL
from lethefield_writer.nodes import ensure_node


class NTracker:
    """per-space n 连续性追踪（M15 消费侧自愈，修订记录第 23 条⑤）。

    冷启动从图内 max n_created 播种（writer 自己的产出口径——重启后仍能检出
    "打分结果发布失败/进程崩溃"留下的缺口；M7 重放重建后播种值同样正确）。
    """

    def __init__(self, gremlin) -> None:
        self._gremlin = gremlin
        self._last: dict[str, int] = {}

    def check(self, space_id: str, n: int) -> range | None:
        """返回缺口 n 区间（空 range 视 None）；无缺口返回 None。"""
        last = self._last.get(space_id)
        if last is None:
            last = self._seed(space_id)
            self._last[space_id] = last
        if n > last + 1:
            return range(last + 1, n)
        return None

    def mark(self, space_id: str, n: int) -> None:
        self._last[space_id] = max(self._last.get(space_id, 0), n)

    def _seed(self, space_id: str) -> int:
        latest = rms_writer.latest_event_node(self._gremlin, space_id, space_id=space_id)
        return latest[1] if latest else 0


class WorkerDeps:
    """worker 依赖容器（构造时注入，测试逐项换 fake）。

    embedder 协议：`embed(text) -> (vector, usage)`（OpenAIEmbedder 同款）。
    control_store：ControlPlaneStore 抽象（list_spaces() 枚举起订集合，红线 1 要件①）。
    quota_counters：共享 QuotaCounters（writer 默认每次现场构造会全图 count，
    批量写入退化成 O(n²)，rebuild 踩坑定案）。
    """

    def __init__(
        self,
        *,
        gremlin,
        es,
        ex_session,
        embedder,
        control_store,
        quota_counters,
        emit: Callable[[LogEvent], None],
        config: WriterConfig = DEFAULT_CONFIG,
    ) -> None:
        self.gremlin = gremlin
        self.es = es
        self.ex_session = ex_session
        self.embedder = embedder
        self.control_store = control_store
        self.quota_counters = quota_counters
        self.emit = emit
        self.config = config


def _compensate_gap(deps: WorkerDeps, space_id: str, gap: range, tracker: NTracker) -> None:
    """按 n 区间从 EX 补偿建点（s 取 EX scoring_result details——全保真档，
    与 M7 重建/M14 SS 同款取数点）。

    details 缺失 = SS 尚未打分（它自己的 n 缺口在 SS 侧补偿）→ 跳过等 SS 补偿
    重发（第 23 条⑤）：正常路径幂等兜底，不重复建点；observation 级日志不 page
    （page 已在缺口检出时发过一次）。
    """
    for ex_event in list_experience_events_range(
        deps.ex_session, space_id=space_id, n_from=gap.start, n_to=gap.stop - 1
    ):
        node_key = node_key_of(ex_event.event_id)
        metas = [
            m
            for m in list_meta_events(deps.ex_session, space_id=space_id, node_key=node_key)
            if m.meta_type == SCORING_RESULT_META_TYPE and m.details
        ]
        if not metas:
            deps.emit(
                LogEvent(
                    service="lethefield-writer",
                    event_type="writer_compensate_pending",
                    space_id=space_id,
                    payload={
                        "n": ex_event.n,
                        "message": "EX 无 scoring_result details（SS 尚未打分），"
                        "跳过等 SS 侧补偿重发",
                    },
                )
            )
            tracker.mark(space_id, ex_event.n)
            continue
        details = parse_scoring_details(metas[-1].details)  # created_at 升序，取最新
        outcome = ensure_node(
            deps,
            space_id=space_id,
            event_id=ex_event.event_id,
            node_key=node_key,
            n=ex_event.n,
            s=details.s,
        )
        NODE_WRITE_TOTAL.labels(result="compensated" if outcome == "created" else "duplicate").inc()
        tracker.mark(space_id, ex_event.n)


def process_message(msg, deps: WorkerDeps, tracker: NTracker) -> None:
    """单条 scoring-results 消息：信封校验 → n 连续性 → 幂等建点。异常上抛（由运行时 nack）。"""
    result = ScoringResult.from_json(msg.data().decode("utf-8"))
    topic_space = space_id_of_topic(msg.topic_name())
    if topic_space != result.space_id:
        raise ValueError(
            f"信封 space_id 与 topic 不符：{result.space_id!r} vs {topic_space!r}（fail-closed）"
        )
    gap = tracker.check(result.space_id, result.n)
    if gap:
        N_GAP_TOTAL.inc()
        deps.emit(
            LogEvent(
                service="lethefield-writer",
                event_type="writer_n_gap",
                space_id=result.space_id,
                payload={
                    "from_n": gap.start,
                    "to_n": gap.stop - 1,
                    "message": "scoring-results n 连续性缺口（发布失败/进程崩溃），"
                    "按 n 区间从 EX 补偿建点",
                },
            )
        )
        _compensate_gap(deps, result.space_id, gap, tracker)
    outcome = ensure_node(
        deps,
        space_id=result.space_id,
        event_id=result.event_id,
        node_key=result.node_key,
        n=result.n,
        s=result.s,
    )
    NODE_WRITE_TOTAL.labels(result=outcome).inc()
    tracker.mark(result.space_id, result.n)


def _subscribe_with_retry(
    client: Client, space_id: str, config: WriterConfig, *, timeout_s: float = 10.0
):
    """订阅单 space 的 scoring-results（durable）。ConsumerBusy 退避重试
    （Exclusive 订阅连接回收滞后，M11 踩坑定案）；其他异常上抛。"""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return client.subscribe(
                scoring_results_topic(space_id),
                SCORING_RESULTS_SUBSCRIPTION,
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
    （InvalidTopicName，namespace 段不允许通配），逐 space 订阅与 SS 同款。
    """

    def __init__(self, client: Client, deps: WorkerDeps) -> None:
        self._client = client
        self._deps = deps
        self._consumers: dict[str, object] = {}
        self._dlq_producers: dict[str, object] = {}
        self._fail_counts: dict[str, int] = {}  # message_id → 连续失败次数（应用层死信计数）
        self._failed_spaces: set[str] = set()  # 订阅失败的 space（恢复时出列）
        self._last_refresh = 0.0
        self._tracker = NTracker(deps.gremlin)
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
                            service="lethefield-writer",
                            event_type="writer_space_subscribe_failed",
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
        worker="rms-writer",
        reason=(
            "枚举走 ControlPlaneStore.list_spaces()（映射表 active 集合）；逐 space 独立 "
            "consumer/独立处理单元（图名 = space_id 单图写入，EX 反查/补偿限定单 "
            "keyspace，无跨 space 联合查询）；批间节流 = topic_discovery_seconds 刷新"
            "间隔 + receive 空轮 timeout"
        ),
        cadence="Pulsar 推送节奏；space 列表刷新节流默认 5s；run_forever 轮间 sleep 1s",
    )
    def run_once(self, *, timeout_ms: int | None = None) -> int:
        """单轮：排空全部 space consumer 当前可达消息并逐条处理（返回处理条数）。

        处理失败：按 message_id 计连续失败次数，未超上限 → nack 留重投（失败不计
        progressed——毒消息重投会把"排空"循环喂成死循环，M14 实测踩坑）；超上限 →
        应用层死信（原文写 DLQ topic + ack + page 级 writer_dlq）。
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
            producer = self._client.create_producer(scoring_results_dlq_topic(space_id))
            self._dlq_producers[space_id] = producer
        producer.send(msg.data())
        consumer.acknowledge(msg)
        self._fail_counts.pop(message_id, None)
        DLQ_TOTAL.inc()
        self._deps.emit(
            LogEvent(
                service="lethefield-writer",
                event_type="writer_dlq",
                space_id=space_id,
                payload={
                    "topic": msg.topic_name(),
                    "failures": fails,
                    "error": f"{type(exc).__name__}: {exc}",
                    "message": "建点重试耗尽，消息已转死信 topic",
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
