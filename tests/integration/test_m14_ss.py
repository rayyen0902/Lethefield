"""M14 SS 显著性打分服务集成测试（开发文档 §15 验收标准）。

真实组件：API 摄入路径（EX 落库 + ex-events 发布）、Pulsar（topic/DLQ）、
cassandra-ex（scoring_result 回写）、JanusGraph（M7 重建全保真档联动）。
LLM 打分用确定性 fake scorer 注入（真实 API 验证走 smoke/validate，不进 CI）。

验收对照：
1. 端到端可追踪：record → ex-events → 打分 → EX scoring_result + scoring-results；
   六维原始值与合成 s 分开存储，权重来自配置。
2. 打分失败 → 重试 → DLQ + page 告警；不丢单不重单。
3. n 连续性缺口 → 告警 + 按 n 区间从 EX 补偿（消费侧自愈）。
4. M7 两档升级：scoring_result 在场时重建 s 全保真。
"""

import time
import uuid
from types import SimpleNamespace

import pulsar
import pytest
import requests
from conftest import GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_api.service import ApiContext, record
from lethefield_api.stream_publisher import ExStreamPublisher
from lethefield_clients import (
    MappingCache,
    MappingTableControlPlaneStore,
    cassandra_cluster,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pulsar_client,
    redis_client,
)
from lethefield_clients.ex_n import list_meta_events
from lethefield_clients.ex_stream import (
    ScoringResult,
    ex_events_dlq_topic,
    ex_events_topic,
    scoring_results_topic,
)
from lethefield_rms import rebuild
from lethefield_rms.schema import SCORING_RESULT_META_TYPE, parse_scoring_details
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_ss.config import SSConfig
from lethefield_ss.llm import ScoringError
from lethefield_ss.worker import ResultPublisher, WorkerDeps, WorkerRuntime

FAKE_MODEL = "fake-scorer-v1"
# fake scorer 的确定性响应（六维固定值 → s = 均权合成 13/30）
FAKE_RAW = '{"er": 0.6, "e": 0.2, "i": 0.8, "g": 0.4, "n": 0.5, "c": 0.1}'
FAKE_S = (0.6 + 0.2 + 0.8 + 0.4 + 0.5 + 0.1) / 6


def _sid(tag: str) -> str:
    return f"m14_{tag}_{uuid.uuid4().hex[:6]}"


class FakeScorer:
    def __init__(self, raw=FAKE_RAW) -> None:
        self.raw = raw
        self.calls: list[str] = []

    def score(self, content: str):
        if isinstance(self.raw, Exception):
            raise self.raw
        self.calls.append(content)
        return self.raw, {"prompt_tokens": 10, "completion_tokens": 5}, FAKE_MODEL


@pytest.fixture(scope="module")
def stack():
    wait_for_gremlin()
    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    cell_session = cell_cluster.connect()
    store = MappingTableControlPlaneStore(cell_session)
    store.ensure_tables()
    try:
        store.get_cell(local_cell().cell_id)
    except KeyError:
        store.register_cell(local_cell())
    pulsar = pulsar_client()
    yield SimpleNamespace(
        store=store,
        cell_session=cell_session,
        ex_session=ex_cluster.connect(),
        gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
        redis=redis_client(),
        pulsar=pulsar,
    )
    pulsar.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()


def _provision(stack, space_id: str) -> None:
    provision_space(
        ProvisionDeps(
            store=stack.store,
            gremlin=stack.gremlin,
            ex_session=stack.ex_session,
            cell_session=stack.cell_session,
        ),
        space_id,
    )


def _api_ctx(stack) -> ApiContext:
    return ApiContext(
        gremlin=None,
        es=None,
        ex_session=stack.ex_session,
        redis=stack.redis,
        meta_appender=lambda **kw: None,
        mapping_cache=MappingCache(stack.store),
        stream_publisher=ExStreamPublisher(stack.pulsar),
    )


def _claims(space_id: str):
    from lethefield_api.auth import Claims

    return Claims("acct-m14", (space_id,), "claude-code", ("record", "flag_conflict"))


def _ss_deps(stack, scorer, emitted: list, **config_over) -> WorkerDeps:
    return WorkerDeps(
        scorer=scorer,
        ex_session=stack.ex_session,
        publisher=ResultPublisher(stack.pulsar),
        control_store=stack.store,
        emit=emitted.append,
        config=SSConfig(
            receive_timeout_ms=200,
            nack_redelivery_delay_ms=50,
            topic_discovery_seconds=1,
            **config_over,
        ),
    )


def _scoring_metas(stack, space_id: str) -> list:
    return [
        m
        for m in list_meta_events(stack.ex_session, space_id=space_id)
        if m.meta_type == SCORING_RESULT_META_TYPE
    ]


def _read_results(stack, space_id: str, timeout_ms: int = 5000) -> list[ScoringResult]:
    """从 scoring-results topic 读回（reader 从 earliest 起，不受订阅位点影响）。"""
    reader = stack.pulsar.create_reader(
        scoring_results_topic(space_id), start_message_id=pulsar.MessageId.earliest
    )
    results = []
    try:
        while True:
            try:
                msg = reader.read_next(timeout_millis=timeout_ms)
            except pulsar.Timeout:
                break
            results.append(ScoringResult.from_json(msg.data().decode("utf-8")))
    finally:
        reader.close()
    return results


def _dlq_topics(stack, space_id: str) -> list[str]:
    resp = requests.get(
        f"http://localhost:8080/admin/v2/namespaces/lethefield/{space_id}/topics", timeout=10
    )
    resp.raise_for_status()
    return [t for t in resp.json() if t.rstrip("/").endswith("-DLQ")]


def _drain_until(stack, space: str, runtime, count: int, timeout_s: float = 90.0) -> None:
    """条件等待：跑 worker 轮直到 EX 出现 count 笔 scoring_result（不赌固定轮数）。"""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        runtime.run_once()
        if len(_scoring_metas(stack, space)) >= count:
            return
        time.sleep(0.2)
    raise AssertionError(
        f"超时：{space} 的 scoring_result 未达 {count} 笔"
        f"（当前 {len(_scoring_metas(stack, space))}）"
    )


# ------------------------------------------- 验收 1：端到端 + 权重来自配置


def test_record_to_scoring_end_to_end(stack):
    space = _sid("e2e")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="我拿到了心仪公司的 offer！")
    r2 = record(ctx, _claims(space), space_id=space, content="今天午饭吃了碗面。")
    assert (r1["n"], r2["n"]) == (1, 2)

    scorer = FakeScorer()
    runtime = WorkerRuntime(stack.pulsar, _ss_deps(stack, scorer, []))
    try:
        _drain_until(stack, space, runtime, 2)
    finally:
        runtime.close()

    metas = _scoring_metas(stack, space)
    assert len(metas) == 2
    by_node = {m.node_key: m for m in metas}
    d1 = parse_scoring_details(by_node[f"ev_{r1['event_id']}"].details)
    assert d1.dims == {"er": 0.6, "e": 0.2, "i": 0.8, "g": 0.4, "n": 0.5, "c": 0.1}
    assert d1.s == pytest.approx(FAKE_S)  # 均权配置合成
    assert d1.model_version == FAKE_MODEL
    assert d1.event_id == r1["event_id"]
    assert d1.degraded is False
    assert by_node[f"ev_{r1['event_id']}"].n_at_event == 1  # 不推进 n，n_at_event 快照

    results = _read_results(stack, space)
    assert {r.event_id for r in results} == {r1["event_id"], r2["event_id"]}
    assert all(r.s == pytest.approx(FAKE_S) for r in results)
    # worker 会顺带消费其他测试 space 的滞留消息（全量套件/多重跑场景），
    # 只断言本 space 两条内容都被打过分
    assert {"我拿到了心仪公司的 offer！", "今天午饭吃了碗面。"} <= set(scorer.calls)


def test_weights_from_config_change_s(stack):
    """权重禁硬编码：同一份六维分值，不同权重配置 → 不同合成 s。"""
    space = _sid("weights")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    record(ctx, _claims(space), space_id=space, content="权重配置验证事件。")
    deps = _ss_deps(
        stack, FakeScorer(), [], weights={"er": 1.0, "e": 0, "i": 0, "g": 0, "n": 0, "c": 0}
    )
    runtime = WorkerRuntime(stack.pulsar, deps)
    try:
        _drain_until(stack, space, runtime, 1)
    finally:
        runtime.close()
    (meta,) = _scoring_metas(stack, space)
    assert parse_scoring_details(meta.details).s == pytest.approx(0.6)  # = er 分值


# ------------------------------------------- 验收 2：幂等（重复投递不重单）


def test_duplicate_delivery_no_duplicate_write(stack):
    space = _sid("idemp")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r = record(ctx, _claims(space), space_id=space, content="幂等验证事件。")

    emitted: list = []
    deps = _ss_deps(stack, FakeScorer(), emitted)
    runtime = WorkerRuntime(stack.pulsar, deps)
    try:
        _drain_until(stack, space, runtime, 1)
        # 同信封重复投递（模拟 at-least-once 重放）
        producer = stack.pulsar.create_producer(ex_events_topic(space))
        from lethefield_clients.ex_stream import ExStreamEvent

        producer.send(
            ExStreamEvent(
                space_id=space,
                event_id=r["event_id"],
                n=r["n"],
                content="幂等验证事件。",
                agent_actor_id="claude-code",
                account_id="acct-m14",
                tau_ms=None,
                ref_conflict=None,
                created_at_ms=int(time.time() * 1000),
            )
            .to_json()
            .encode("utf-8")
        )
        producer.close()
        # 重复消息被消费（下游补发）前可能有数轮空跑——按结果 topic 消息数等
        deadline = time.time() + 60
        while time.time() < deadline:
            runtime.run_once()
            if len(_read_results(stack, space, timeout_ms=500)) >= 2:
                break
            time.sleep(0.2)
    finally:
        runtime.close()
    assert len(_scoring_metas(stack, space)) == 1  # EX 侧仍只有一笔（不重单）
    results = _read_results(stack, space)
    assert len(results) == 2  # 下游补发允许重复（M15 ref_ex 幂等已定案）
    assert {r.event_id for r in results} == {r["event_id"]}


# ------------------------------------------- 验收 3：失败 → 重试 → DLQ + 告警


def test_scoring_failure_retries_then_dlq(stack):
    space = _sid("dlq")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    record(ctx, _claims(space), space_id=space, content="DLQ 验证事件。")

    emitted: list = []
    scorer = FakeScorer(ScoringError("LLM 超时（fake）"))
    deps = _ss_deps(stack, scorer, emitted, max_redeliver_count=1)
    runtime = WorkerRuntime(stack.pulsar, deps)
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            runtime.run_once()
            if any(e.event_type == "ss_scoring_dlq" for e in emitted):
                break
            time.sleep(0.3)
    finally:
        runtime.close()

    dlq_events = [e for e in emitted if e.event_type == "ss_scoring_dlq"]
    assert dlq_events, "重试耗尽应有 page 级 ss_scoring_dlq 告警"
    assert _scoring_metas(stack, space) == []  # 失败不写 EX

    # 死信落地（不丢单）：DLQ topic 出现且消息原文可读回（应用层转移，命名单点）
    deadline = time.time() + 30
    dlqs = []
    while time.time() < deadline:
        dlqs = _dlq_topics(stack, space)
        if dlqs:
            break
        time.sleep(0.5)
    assert dlqs == [ex_events_dlq_topic(space)], "DLQ topic 应按命名单点出现"
    reader = stack.pulsar.create_reader(dlqs[0], start_message_id=pulsar.MessageId.earliest)
    try:
        msg = reader.read_next(timeout_millis=10000)
        assert "DLQ 验证事件" in msg.data().decode("utf-8")
    finally:
        reader.close()


# ------------------------------------------- 验收 4：n 连续性缺口补偿


def test_n_gap_compensation_from_ex(stack):
    space = _sid("gap")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    # n=1：绕开 Pulsar 发布（模拟"EX 落库成功但发布失败"）
    from lethefield_api import ex_ingest

    e1_id, n1 = ex_ingest.append_experience(
        stack.ex_session,
        stack.redis,
        space_id=space,
        content="丢失发布的事件一。",
        agent_actor_id="claude-code",
        account_id="acct-m14",
    )
    assert n1 == 1
    # n=2：正常 record（发布会带出缺口）
    r2 = record(ctx, _claims(space), space_id=space, content="正常事件二。")
    assert r2["n"] == 2

    emitted: list = []
    runtime = WorkerRuntime(stack.pulsar, _ss_deps(stack, FakeScorer(), emitted))
    try:
        _drain_until(stack, space, runtime, 2)
    finally:
        runtime.close()

    gaps = [e for e in emitted if e.event_type == "ss_n_gap" and e.space_id == space]
    assert gaps and gaps[0].payload["from_n"] == 1 and gaps[0].payload["to_n"] == 1
    metas = {m.node_key: m for m in _scoring_metas(stack, space)}
    assert f"ev_{e1_id}" in metas  # 缺口事件已补偿打分
    assert f"ev_{r2['event_id']}" in metas


# ------------------------------------------- 验收 5：M7 重建全保真档联动


def test_rebuild_reads_scoring_result(stack):
    """M14 落地后两档切换：scoring_result 在场 → 重建初始 s 全保真（非占位常数）。"""
    space = _sid("rebuild")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="重建保真事件一。")
    r2 = record(ctx, _claims(space), space_id=space, content="重建保真事件二。")
    runtime = WorkerRuntime(stack.pulsar, _ss_deps(stack, FakeScorer(), []))
    try:
        _drain_until(stack, space, runtime, 2)
    finally:
        runtime.close()

    target = f"{space}_rb"
    plan = rebuild.rebuild_space(
        stack.gremlin,
        stack.cell_session,
        stack.ex_session,
        space_id=space,
        target_gname=target,
    )
    try:
        by_key = {n.node_key: n for n in plan.nodes}
        assert by_key[f"ev_{r1['event_id']}"].s == pytest.approx(FAKE_S)  # 全保真
        assert by_key[f"ev_{r2['event_id']}"].s == pytest.approx(FAKE_S)
        assert by_key[f"ev_{r1['event_id']}"].s != rebuild.PLACEHOLDER_S
    finally:
        stack.gremlin.submit(
            "try { ConfiguredGraphFactory.close(gname) } catch (ignored) {}; 'closed'",
            {"gname": target},
        ).all().result()
