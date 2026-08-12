"""worker 单测（M14）：正常链路、EX 幂等、n 缺口补偿、失败 nack 与 DLQ 前夕告警。"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pulsar
import pytest
from lethefield_clients.ex_stream import ExStreamEvent
from lethefield_rms.schema import (
    SCORING_RESULT_META_TYPE,
    parse_scoring_details,
    scoring_details_of,
)
from lethefield_ss.config import SSConfig
from lethefield_ss.llm import ScoringError
from lethefield_ss.worker import NTracker, WorkerDeps, WorkerRuntime, process_message


class FakeScorer:
    def __init__(self, raw: str | Exception = "{}", model: str = "fake-v1") -> None:
        self.raw = raw
        self.model = model
        self.calls: list[str] = []

    def score(self, content: str):
        if isinstance(self.raw, Exception):
            raise self.raw
        self.calls.append(content)
        return self.raw, {"prompt_tokens": 10, "completion_tokens": 5}, self.model


FULL = '{"er": 0.6, "e": 0.2, "i": 0.8, "g": 0.4, "n": 0.5, "c": 0.1}'


class FakePublisher:
    def __init__(self) -> None:
        self.published: list = []

    def publish(self, result) -> None:
        self.published.append(result)


class FakeExSession:
    """内存 EX：meta_events 行集（append_meta_row 参数位序）+ experience_events 预置。"""

    def __init__(self) -> None:
        self.metas: dict[str, list] = {}
        self.events: dict[str, list] = {}

    def seed_event(self, space_id: str, n: int, event_id: str) -> None:
        self.events.setdefault(space_id, []).append(
            SimpleNamespace(
                n=n,
                event_id=event_id,
                content=f"补偿内容-{n}",
                agent_actor_id="a",
                account_id="acc",
                tau_ms=None,
                ref_conflict=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    def execute(self, query: str, params: tuple = ()):
        space = query.split("ex_")[1].split(".")[0]
        if query.strip().startswith("INSERT"):
            row = SimpleNamespace(
                node_key=params[0],
                created_at=params[1],
                event_id=params[2],
                meta_type=params[3],
                count=params[4],
                n_at_event=params[5],
                agent_actor_id=params[6],
                account_id=params[7],
                details=params[8],
            )
            self.metas.setdefault(space, []).append(row)
            return SimpleNamespace(all=lambda: [])
        if "meta_events" in query:
            rows = self.metas.get(space, [])
            if "WHERE node_key" in query:
                rows = [r for r in rows if r.node_key == params[0]]
            return SimpleNamespace(all=lambda: rows)
        return SimpleNamespace(all=lambda: self.events.get(space, []))


class FakeMsg:
    _seq = 0

    def __init__(self, event: ExStreamEvent, message_id: str | None = None) -> None:
        self._event = event
        if message_id is None:
            FakeMsg._seq += 1
            message_id = f"mid-{FakeMsg._seq}"
        self._message_id = message_id

    def data(self) -> bytes:
        return self._event.to_json().encode("utf-8")

    def topic_name(self) -> str:
        return f"persistent://lethefield/{self._event.space_id}/ex-events"

    def message_id(self) -> str:
        return self._message_id


class FakeConsumer:
    def __init__(self, msgs: list) -> None:
        self.msgs = list(msgs)
        self.acked: list = []
        self.nacked: list = []

    def receive(self, timeout_millis: int):
        if not self.msgs:
            raise pulsar.Timeout
        return self.msgs.pop(0)

    def acknowledge(self, msg) -> None:
        self.acked.append(msg)

    def negative_acknowledge(self, msg) -> None:
        self.nacked.append(msg)

    def close(self) -> None:
        pass


class FakeDlqProducer:
    def __init__(self) -> None:
        self.sent: list[bytes] = []

    def send(self, payload: bytes) -> None:
        self.sent.append(payload)

    def close(self) -> None:
        pass


class FakeClient:
    def __init__(self, consumer: FakeConsumer) -> None:
        self.consumer = consumer
        self.subscribe_args: list = []
        self.dlq_producer = FakeDlqProducer()

    def subscribe(self, topic, subscription_name, **kwargs):
        self.subscribe_args.append((topic, subscription_name, kwargs))
        return self.consumer

    def create_producer(self, topic: str) -> FakeDlqProducer:
        return self.dlq_producer


class FakeControlStore:
    def __init__(self, spaces: list[str]) -> None:
        self._spaces = spaces

    def list_spaces(self) -> list[str]:
        return self._spaces


def _event(space_id: str = "demo", event_id: str = "e1", n: int = 1) -> ExStreamEvent:
    return ExStreamEvent(
        space_id=space_id,
        event_id=event_id,
        n=n,
        content="内容",
        agent_actor_id="a",
        account_id="acc",
        tau_ms=None,
        ref_conflict=None,
        created_at_ms=1,
    )


def _deps(
    session: FakeExSession, scorer=None, spaces=("demo",)
) -> tuple[WorkerDeps, FakePublisher, list]:
    publisher = FakePublisher()
    events: list = []
    deps = WorkerDeps(
        scorer=scorer or FakeScorer(FULL),
        ex_session=session,
        publisher=publisher,
        control_store=FakeControlStore(list(spaces)),
        emit=events.append,
        config=SSConfig(),
    )
    return deps, publisher, events


def test_process_message_full_chain():
    session = FakeExSession()
    deps, publisher, _ = _deps(session)
    process_message(FakeMsg(_event()), deps, NTracker(session))
    # EX 回写：scoring_result 元事件（不推进 n 的纯 INSERT）
    rows = session.metas["demo"]
    assert len(rows) == 1
    row = rows[0]
    assert row.meta_type == SCORING_RESULT_META_TYPE
    assert row.node_key == "ev_e1" and row.n_at_event == 1
    details = parse_scoring_details(row.details)
    assert details.s == pytest.approx((0.6 + 0.2 + 0.8 + 0.4 + 0.5 + 0.1) / 6)
    assert details.dims["er"] == 0.6 and details.event_id == "e1"
    assert details.model_version == "fake-v1" and details.degraded is False
    # 下游发布：信封一致
    assert len(publisher.published) == 1
    assert publisher.published[0].event_id == "e1"


def test_process_message_idempotent_skip_rescore():
    """EX 已有 scoring_result：不调 LLM（不重打分），从 details 重建信封补发下游。"""
    session = FakeExSession()
    scorer = FakeScorer(FULL)
    deps, publisher, _ = _deps(session, scorer)
    node_key = "ev_e1"
    session.metas["demo"] = [
        SimpleNamespace(
            node_key=node_key,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            event_id="m1",
            meta_type=SCORING_RESULT_META_TYPE,
            count=1,
            n_at_event=1,
            agent_actor_id=None,
            account_id=None,
            details=scoring_details_of(
                dims={d: 0.5 for d in ("er", "e", "i", "g", "n", "c")},
                s=0.5,
                model_version="old-v0",
                event_id="e1",
            ),
        )
    ]
    process_message(FakeMsg(_event()), deps, NTracker(session))
    assert scorer.calls == []  # 未重打分
    assert len(session.metas["demo"]) == 1  # 未重复回写
    assert publisher.published[0].model_version == "old-v0"  # 从 details 重建


def test_n_gap_triggers_compensation():
    """n 缺口：page 告警 + 按 n 区间从 EX 补偿打分（消费侧自愈）。"""
    session = FakeExSession()
    session.seed_event("demo", 1, "e1")
    session.seed_event("demo", 2, "e2")
    scorer = FakeScorer(FULL)
    deps, publisher, events = _deps(session, scorer)
    # 直接收到 n=3（1、2 的发布丢失）：冷启动 seed=0 → 缺口 {1,2}
    process_message(FakeMsg(_event(event_id="e3", n=3)), deps, NTracker(session))
    assert events[0].event_type == "ss_n_gap"
    assert events[0].payload["from_n"] == 1 and events[0].payload["to_n"] == 2
    assert len(scorer.calls) == 3  # 补偿 1、2 + 当前 3
    assert len(session.metas["demo"]) == 3
    assert len(publisher.published) == 3


def test_no_gap_when_continuous():
    session = FakeExSession()
    deps, _, events = _deps(session)
    tracker = NTracker(session)
    process_message(FakeMsg(_event(n=1, event_id="e1")), deps, tracker)
    process_message(FakeMsg(_event(n=2, event_id="e2")), deps, tracker)
    assert events == []  # 无缺口告警
    assert len(session.metas["demo"]) == 2


def test_space_mismatch_fail_closed():
    session = FakeExSession()
    deps, _, _ = _deps(session)
    bad = _event(space_id="demo")
    msg = FakeMsg(bad)
    msg.topic_name = lambda: "persistent://lethefield/other/ex-events"  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="space_id 与 topic 不符"):
        process_message(msg, deps, NTracker(session))


def test_runtime_nack_on_failure_and_ack_on_success():
    session = FakeExSession()
    scorer = FakeScorer(ScoringError("LLM 超时（fake）"))
    deps, _, events = _deps(session, scorer)
    msg = FakeMsg(_event())
    consumer = FakeConsumer([msg])
    runtime = WorkerRuntime(FakeClient(consumer), deps)
    try:
        assert runtime.run_once() == 0
    finally:
        runtime.close()
    assert consumer.nacked == [msg] and consumer.acked == []
    assert events == []  # 未达重投上限：无 DLQ page 事件


def test_runtime_dlq_transfer_after_retries_exhausted():
    """连续失败超上限：原文转死信 topic + ack 原消息 + page 级 ss_scoring_dlq。"""
    session = FakeExSession()
    publisher = FakePublisher()
    events: list = []
    deps = WorkerDeps(
        scorer=FakeScorer(ScoringError("LLM 超时（fake）")),
        ex_session=session,
        publisher=publisher,
        control_store=FakeControlStore(["demo"]),
        emit=events.append,
        config=SSConfig(max_redeliver_count=2),
    )
    msg = FakeMsg(_event(), message_id="mid-poison")
    # 同一 message_id 重投三次（1、2 次 nack，第 3 次超上限转死信）；
    # 纯失败轮不算 progressed，run_once 每轮只吃一条重投——逐轮推进
    consumer = FakeConsumer([msg, msg, msg])
    client = FakeClient(consumer)
    runtime = WorkerRuntime(client, deps)
    try:
        for _ in range(3):
            runtime.run_once()
    finally:
        runtime.close()
    assert consumer.nacked == [msg, msg]  # 前两次 nack 重投
    assert consumer.acked == [msg]  # 第三次：死信转移后 ack（不重单）
    assert len(client.dlq_producer.sent) == 1  # 原文进死信 topic（不丢单）
    assert ExStreamEvent.from_json(client.dlq_producer.sent[0].decode()).event_id == "e1"
    assert events[0].event_type == "ss_scoring_dlq"
    assert events[0].payload["failures"] == 3


def test_runtime_subscribes_per_active_space():
    """space 发现：list_spaces() 枚举起订，逐 space 一个 consumer（红线 1 合规形态）。"""
    session = FakeExSession()
    deps, _, _ = _deps(session, spaces=("sp_a", "sp_b"))
    client = FakeClient(FakeConsumer([]))
    runtime = WorkerRuntime(client, deps)
    try:
        topics = sorted(args[0] for args in client.subscribe_args)
        assert topics == [
            "persistent://lethefield/sp_a/ex-events",
            "persistent://lethefield/sp_b/ex-events",
        ]
    finally:
        runtime.close()


def test_runtime_skips_unsubscribable_space():
    """namespace/topic 未就绪的 space：跳过 + observation 日志，不影响其他 space。"""

    class FlakyClient(FakeClient):
        def subscribe(self, topic, subscription_name, **kwargs):
            if "broken" in topic:
                raise RuntimeError("TopicNotFound（fake）")
            return super().subscribe(topic, subscription_name, **kwargs)

    session = FakeExSession()
    deps, _, events = _deps(session, spaces=("demo", "broken"))
    runtime = WorkerRuntime(FlakyClient(FakeConsumer([])), deps)
    try:
        assert set(runtime._consumers) == {"demo"}  # broken 被跳过
        subscribe_failures = [e for e in events if e.event_type == "ss_space_subscribe_failed"]
        assert len(subscribe_failures) == 1 and subscribe_failures[0].space_id == "broken"
    finally:
        runtime.close()
