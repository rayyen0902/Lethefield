"""writer worker 单测（M15）：建点幂等三分解、时序边链、n 缺口补偿、失败 nack/DLQ。

无栈：FakeGremlin（内存图）/ FakeEs（内存文档）/ FakeExSession（内存 EX 行集）
/ FakeEmbedder——编排逻辑全 fake 验证，真实组件行为归集成测试。
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pulsar
import pytest
from lethefield_clients.ex_stream import ScoringResult
from lethefield_rms.schema import SCORING_RESULT_META_TYPE, scoring_details_of
from lethefield_writer.config import WriterConfig
from lethefield_writer.embedding import EmbedError
from lethefield_writer.nodes import ExEventMissing
from lethefield_writer.worker import NTracker, WorkerDeps, WorkerRuntime, process_message

DIMS = ("er", "e", "i", "g", "n", "c")


class _R:
    def __init__(self, value):
        self._value = value

    def all(self):
        return self

    def result(self):
        return self._value


class FakeGremlin:
    """内存图：按脚本特征分发（写脚本落内存，查询脚本按 bindings 回答）。"""

    def __init__(self) -> None:
        self.vertices: dict[str, dict] = {}  # space -> {node_key: props}
        self.edges: list[tuple] = []  # (space, from_key, to_key, label)
        self.submits: list[tuple[str, dict]] = []

    def submit(self, script, bindings=None):
        self.submits.append((script, bindings))
        space = bindings["spaceId"]
        if "addV" in script:
            self.vertices.setdefault(space, {})[bindings["nodeKey"]] = {
                "n_created": int(bindings["nCreated"]),
                "n_last_touched": int(bindings["nCreated"]),
                "content": bindings["contentText"],
                "ref_ex": bindings["refEx"],
                "s": bindings["sVal"],
                "agent_actor_id": bindings.get("actorId"),
            }
            return _R(["ok"])
        if "addEdge" in script:
            self.edges.append(
                (space, bindings["fromKey"], bindings["toKey"], bindings["edgeLabel"])
            )
            return _R(["ok"])
        if "project" in script:
            before = bindings.get("beforeN")
            before = int(before) if before is not None else None
            cands = [
                (k, v["n_created"])
                for k, v in self.vertices.get(space, {}).items()
                if before is None or v["n_created"] < before
            ]
            if not cands:
                return _R([])
            key, n = max(cands, key=lambda kv: kv[1])
            # 真实服务端按元素逐个流回（非嵌套列表）
            return _R([{"node_key": key, "n_created": n}])
        if "outE('temporal')" in script:
            found = any(
                e == (space, bindings["fromKey"], bindings["toKey"], "temporal") for e in self.edges
            )
            return _R([found])
        return _R([bindings["nodeKey"] in self.vertices.get(space, {})])  # vertex_exists

    def add_v_calls(self) -> int:
        return sum(1 for script, _ in self.submits if "addV" in script)


class FakeEs:
    """内存 rms_vectors：doc id = {space}:{node_key}（index_vector 约定）。"""

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}

    def options(self, **kwargs):
        return self

    def get(self, *, index, id, routing):
        if id in self.docs:
            return {"found": True, "_source": self.docs[id]}
        return {"found": False}

    def index(self, *, index, id, document, routing, refresh):
        assert routing == document["space_id"]  # routing = space_id 双机制之一
        self.docs[id] = document


class FakeExSession:
    """内存 EX：experience_events / meta_events 行集（按 CQL 文本分发）。"""

    def __init__(self) -> None:
        self.events: dict[str, list] = {}
        self.metas: dict[str, list] = {}

    def seed_event(self, space_id: str, n: int, event_id: str, *, actor: str = "a") -> None:
        self.events.setdefault(space_id, []).append(
            SimpleNamespace(
                n=n,
                event_id=event_id,
                content=f"内容-{n}",
                agent_actor_id=actor,
                account_id="acc",
                tau_ms=1000 * n,
                ref_conflict=None,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )

    def seed_scoring_meta(self, space_id: str, event_id: str, n: int, s: float) -> None:
        self.metas.setdefault(space_id, []).append(
            SimpleNamespace(
                node_key=f"ev_{event_id}",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                event_id=f"m{n}",
                meta_type=SCORING_RESULT_META_TYPE,
                count=1,
                n_at_event=n,
                agent_actor_id=None,
                account_id=None,
                details=scoring_details_of(
                    dims={d: 0.5 for d in DIMS}, s=s, model_version="fake-v1", event_id=event_id
                ),
            )
        )

    def execute(self, query: str, params: tuple = ()):
        space = query.split("ex_")[1].split(".")[0]
        if "meta_events" in query:
            rows = self.metas.get(space, [])
            if "WHERE node_key" in query:
                rows = [r for r in rows if r.node_key == params[0]]
            return SimpleNamespace(all=lambda: rows)
        rows = self.events.get(space, [])
        if "WHERE n = %s" in query:
            hit = [r for r in rows if r.n == params[0]]
            return SimpleNamespace(one=lambda: hit[0] if hit else None)
        return SimpleNamespace(all=lambda: rows)


class FakeEmbedder:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    def embed(self, text: str):
        if self.error is not None:
            raise self.error
        self.calls.append(text)
        return [0.1, 0.2, 0.3, 0.4], {"prompt_tokens": 3, "total_tokens": 3}


class FakeCounters:
    def vertex_count(self, gname) -> int:
        return 0

    def edge_count(self, gname) -> int:
        return 0

    def vector_count(self, space_id) -> int:
        return 0


class FakeMsg:
    _seq = 0

    def __init__(self, result: ScoringResult, message_id: str | None = None) -> None:
        self._result = result
        if message_id is None:
            FakeMsg._seq += 1
            message_id = f"mid-{FakeMsg._seq}"
        self._message_id = message_id

    def data(self) -> bytes:
        return self._result.to_json().encode("utf-8")

    def topic_name(self) -> str:
        return f"persistent://lethefield/{self._result.space_id}/scoring-results"

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


def _result(
    space_id: str = "demo", event_id: str = "e1", n: int = 1, s: float = 0.7
) -> ScoringResult:
    return ScoringResult(
        space_id=space_id,
        event_id=event_id,
        n=n,
        node_key=f"ev_{event_id}",
        dims={d: 0.5 for d in DIMS},
        s=s,
        model_version="fake-v1",
        degraded=False,
    )


def _deps(
    gremlin: FakeGremlin,
    es: FakeEs,
    session: FakeExSession,
    embedder=None,
    spaces=("demo",),
    config: WriterConfig | None = None,
) -> tuple[WorkerDeps, list]:
    events: list = []
    deps = WorkerDeps(
        gremlin=gremlin,
        es=es,
        ex_session=session,
        embedder=embedder or FakeEmbedder(),
        control_store=FakeControlStore(list(spaces)),
        quota_counters=FakeCounters(),
        emit=events.append,
        config=config or WriterConfig(),
    )
    return deps, events


def test_process_message_creates_node():
    """全新事件：顶点字段级（含 φ 初始化与 A_i 盖章列）+ 向量落库；首节点无时序边。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1", actor="writer-1")
    embedder = FakeEmbedder()
    deps, _ = _deps(gremlin, es, session, embedder)
    process_message(FakeMsg(_result()), deps, NTracker(gremlin))
    vertex = gremlin.vertices["demo"]["ev_e1"]
    assert vertex["content"] == "内容-1"  # c_i 反查 EX
    assert vertex["ref_ex"] == "e1"
    assert vertex["s"] == 0.7  # s 取自信封（SS 合成初值）
    assert vertex["n_created"] == 1 == vertex["n_last_touched"]
    assert vertex["agent_actor_id"] == "writer-1"  # A_i 来自 EX 盖章列
    assert gremlin.edges == []  # 首节点无前驱
    doc = es.docs["demo:ev_e1"]
    assert doc["node_key"] == "ev_e1" and doc["content"] == "内容-1"
    assert embedder.calls == ["内容-1"]


def test_duplicate_delivery_zero_writes():
    """重复投递：顶点/边/向量三分解全在 → duplicate 零写入（不重复建点、不重复 embed）。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1")
    session.seed_event("demo", 2, "e2")
    embedder = FakeEmbedder()
    deps, _ = _deps(gremlin, es, session, embedder)
    tracker = NTracker(gremlin)
    process_message(FakeMsg(_result(n=1, event_id="e1")), deps, tracker)
    process_message(FakeMsg(_result(n=2, event_id="e2")), deps, tracker)
    assert len(gremlin.edges) == 1
    # 重复投递 n=2：零副作用
    process_message(FakeMsg(_result(n=2, event_id="e2")), deps, tracker)
    process_message(FakeMsg(_result(n=2, event_id="e2")), deps, tracker)
    assert len(gremlin.vertices["demo"]) == 2
    assert len(gremlin.edges) == 1
    assert len(es.docs) == 2
    assert embedder.calls == ["内容-1", "内容-2"]  # 无重复 embed 调用


def test_partial_failure_repair():
    """部分失败补全：顶点已建、边/向量缺失 → 补边补向量，不重建顶点。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1")
    session.seed_event("demo", 2, "e2")
    embedder = FakeEmbedder()
    deps, _ = _deps(gremlin, es, session, embedder)
    tracker = NTracker(gremlin)
    process_message(FakeMsg(_result(n=1, event_id="e1")), deps, tracker)
    # 模拟"建点成功但崩在建边/向量前"：直接落 n=2 顶点（绕过 ensure_node）
    gremlin.vertices["demo"]["ev_e2"] = {"n_created": 2}
    process_message(FakeMsg(_result(n=2, event_id="e2")), deps, tracker)
    assert gremlin.add_v_calls() == 1  # ev_e2 未重建
    assert gremlin.edges == [("demo", "ev_e1", "ev_e2", "temporal")]  # 边补上
    assert "demo:ev_e2" in es.docs  # 向量补上


def test_temporal_chain():
    """连续事件按 n 序链式连接（temporal 边）。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    for i in (1, 2, 3):
        session.seed_event("demo", i, f"e{i}")
    deps, _ = _deps(gremlin, es, session)
    tracker = NTracker(gremlin)
    for i in (1, 2, 3):
        process_message(FakeMsg(_result(n=i, event_id=f"e{i}")), deps, tracker)
    assert gremlin.edges == [
        ("demo", "ev_e1", "ev_e2", "temporal"),
        ("demo", "ev_e2", "ev_e3", "temporal"),
    ]


def test_node_key_mismatch_fail_closed():
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1")
    deps, _ = _deps(gremlin, es, session)
    bad = ScoringResult(
        space_id="demo",
        event_id="e1",
        n=1,
        node_key="ev_other",  # 与 node_key_of("e1") 不符
        dims={d: 0.5 for d in DIMS},
        s=0.5,
        model_version="fake-v1",
        degraded=False,
    )
    with pytest.raises(ValueError, match="node_key 与 event_id 不符"):
        process_message(FakeMsg(bad), deps, NTracker(gremlin))
    assert gremlin.vertices == {}  # fail-closed 零写入


def test_ex_event_missing_raises():
    """EX 查无该 n 的经验事件 = 上游不一致 → 失败路径（运行时 nack/DLQ）。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    deps, _ = _deps(gremlin, es, session)
    with pytest.raises(ExEventMissing, match="EX 查无经验事件"):
        process_message(FakeMsg(_result()), deps, NTracker(gremlin))
    assert gremlin.vertices == {}


def test_ex_event_id_mismatch_fail_closed():
    """信封 event_id 与 EX 行不符（同 n 不同事件）→ fail-closed。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "real-id")
    deps, _ = _deps(gremlin, es, session)
    with pytest.raises(ValueError, match="event_id 与 EX 行不符"):
        process_message(FakeMsg(_result(event_id="e1")), deps, NTracker(gremlin))


def test_n_gap_compensation():
    """n 缺口：page 告警 + 按 n 区间从 EX 补偿建点（s 取 scoring_result details）。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    for i in (1, 2, 3):
        session.seed_event("demo", i, f"e{i}")
    session.seed_scoring_meta("demo", "e1", 1, 0.31)
    session.seed_scoring_meta("demo", "e2", 2, 0.62)
    deps, events = _deps(gremlin, es, session)
    # 直接收到 n=3（1、2 的 scoring-results 发布丢失）：冷启动 seed=0 → 缺口 {1,2}
    process_message(FakeMsg(_result(n=3, event_id="e3")), deps, NTracker(gremlin))
    assert events[0].event_type == "writer_n_gap"
    assert events[0].payload["from_n"] == 1 and events[0].payload["to_n"] == 2
    assert set(gremlin.vertices["demo"]) == {"ev_e1", "ev_e2", "ev_e3"}
    assert gremlin.vertices["demo"]["ev_e1"]["s"] == 0.31  # 补偿 s 取 EX details 全保真
    assert gremlin.vertices["demo"]["ev_e2"]["s"] == 0.62
    assert gremlin.edges == [
        ("demo", "ev_e1", "ev_e2", "temporal"),
        ("demo", "ev_e2", "ev_e3", "temporal"),
    ]


def test_n_gap_compensate_pending_when_details_missing():
    """缺口事件 SS 尚未打分（EX 无 scoring_result details）→ 跳过等 SS 补偿重发。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    for i in (1, 2, 3):
        session.seed_event("demo", i, f"e{i}")
    # 只有 e1 有 details，e2 没有
    session.seed_scoring_meta("demo", "e1", 1, 0.5)
    deps, events = _deps(gremlin, es, session)
    process_message(FakeMsg(_result(n=3, event_id="e3")), deps, NTracker(gremlin))
    # e3 需要在 EX（正常路径反查）
    assert "ev_e2" not in gremlin.vertices.get("demo", {})  # 未打分不建点
    pending = [e for e in events if e.event_type == "writer_compensate_pending"]
    assert len(pending) == 1 and pending[0].payload["n"] == 2


def test_no_gap_when_continuous():
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    for i in (1, 2):
        session.seed_event("demo", i, f"e{i}")
    deps, events = _deps(gremlin, es, session)
    tracker = NTracker(gremlin)
    process_message(FakeMsg(_result(n=1, event_id="e1")), deps, tracker)
    process_message(FakeMsg(_result(n=2, event_id="e2")), deps, tracker)
    assert events == []  # 无缺口告警


def test_space_mismatch_fail_closed():
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    deps, _ = _deps(gremlin, es, session)
    msg = FakeMsg(_result(space_id="demo"))
    msg.topic_name = lambda: "persistent://lethefield/other/scoring-results"  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="space_id 与 topic 不符"):
        process_message(msg, deps, NTracker(gremlin))


def test_runtime_nack_on_failure_and_ack_on_success():
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1")
    deps, events = _deps(gremlin, es, session, FakeEmbedder(EmbedError("嵌入超时（fake）")))
    msg = FakeMsg(_result())
    consumer = FakeConsumer([msg])
    runtime = WorkerRuntime(FakeClient(consumer), deps)
    try:
        assert runtime.run_once() == 0
    finally:
        runtime.close()
    assert consumer.nacked == [msg] and consumer.acked == []
    assert events == []  # 未达重投上限：无 DLQ page 事件


def test_runtime_dlq_transfer_after_retries_exhausted():
    """连续失败超上限：原文转死信 topic + ack 原消息 + page 级 writer_dlq。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    session.seed_event("demo", 1, "e1")
    deps, events = _deps(
        gremlin,
        es,
        session,
        FakeEmbedder(EmbedError("嵌入超时（fake）")),
        config=WriterConfig(max_redeliver_count=2),
    )
    msg = FakeMsg(_result(), message_id="mid-poison")
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
    assert ScoringResult.from_json(client.dlq_producer.sent[0].decode()).event_id == "e1"
    dlq_events = [e for e in events if e.event_type == "writer_dlq"]
    assert len(dlq_events) == 1 and dlq_events[0].payload["failures"] == 3


def test_runtime_subscribes_per_active_space():
    """space 发现：list_spaces() 枚举起订，逐 space 一个 consumer（红线 1 合规形态）。"""
    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    deps, _ = _deps(gremlin, es, session, spaces=("sp_a", "sp_b"))
    client = FakeClient(FakeConsumer([]))
    runtime = WorkerRuntime(client, deps)
    try:
        topics = sorted(args[0] for args in client.subscribe_args)
        assert topics == [
            "persistent://lethefield/sp_a/scoring-results",
            "persistent://lethefield/sp_b/scoring-results",
        ]
        assert all(args[1] == "rms-writer" for args in client.subscribe_args)
    finally:
        runtime.close()


def test_runtime_skips_unsubscribable_space():
    """namespace/topic 未就绪的 space：跳过 + observation 日志，不影响其他 space。"""

    class FlakyClient(FakeClient):
        def subscribe(self, topic, subscription_name, **kwargs):
            if "broken" in topic:
                raise RuntimeError("TopicNotFound（fake）")
            return super().subscribe(topic, subscription_name, **kwargs)

    gremlin, es, session = FakeGremlin(), FakeEs(), FakeExSession()
    deps, events = _deps(gremlin, es, session, spaces=("demo", "broken"))
    runtime = WorkerRuntime(FlakyClient(FakeConsumer([])), deps)
    try:
        assert set(runtime._consumers) == {"demo"}  # broken 被跳过
        failures = [e for e in events if e.event_type == "writer_space_subscribe_failed"]
        assert len(failures) == 1 and failures[0].space_id == "broken"
    finally:
        runtime.close()
