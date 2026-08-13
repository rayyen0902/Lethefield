"""M15 写入链 worker 集成测试（开发文档 §16 验收标准）。

真实组件：API 摄入路径（EX 落库 + ex-events 发布）、Pulsar（scoring-results/DLQ）、
cassandra-ex（EX 反查/补偿取数）、JanusGraph（顶点/时序边）、ES（rms_vectors）。
embedding 用确定性 fake 注入（4 维；真实 provider 冒烟不属 CI——DeepSeek 无
embeddings 端点，LETHEFIELD_EMBED_* 真实冒烟前需另配 provider，修订记录 23 条④）。

验收对照：
1. 端到端：record → SS 打分 → writer 建点，字段级断言（c_i/τ_i/A_i/ref_ex/φ 初值）。
2. 幂等：同一打分结果重复投递 N 次，1 顶点、1 时序边、1 向量文档。
3. 元事件（reinforce）不建经验顶点、不推进 n_now。
4. 向量与图顶点一一对应（node_key 关联）+ 跨 space routing 隔离。
5. n 连续性缺口 → page 告警 + 按 n 区间从 EX 补偿（s 取 scoring_result details）。
6. 嵌入失败 → 重试 → 应用层 DLQ + page 告警。
"""

import time
import uuid
from datetime import UTC
from functools import partial
from types import SimpleNamespace

import pulsar
import pytest
import requests
from conftest import GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_api import ex_ingest
from lethefield_api.service import ApiContext, record, reinforce
from lethefield_api.stream_publisher import ExStreamPublisher
from lethefield_clients import (
    MappingCache,
    MappingTableControlPlaneStore,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pulsar_client,
    redis_client,
)
from lethefield_clients.ex_n import (
    append_meta_row,
    get_experience_event,
    list_meta_events,
    n_now,
)
from lethefield_clients.ex_stream import (
    ScoringResult,
    scoring_results_dlq_topic,
    scoring_results_topic,
)
from lethefield_rms.ff import DEFAULT_CONFIG as FF_CONFIG
from lethefield_rms.ff import n_star_horizon
from lethefield_rms.quota import QuotaCounters
from lethefield_rms.rebuild import node_key_of
from lethefield_rms.schema import SCORING_RESULT_META_TYPE, scoring_details_of
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, knn_search
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_ss.config import SSConfig
from lethefield_ss.worker import ResultPublisher
from lethefield_ss.worker import WorkerDeps as SSWorkerDeps
from lethefield_ss.worker import WorkerRuntime as SSWorkerRuntime
from lethefield_writer.config import WriterConfig
from lethefield_writer.embedding import EmbedError
from lethefield_writer.worker import WorkerDeps, WorkerRuntime

FAKE_MODEL = "fake-scorer-v1"
FAKE_DIMS = {"er": 0.6, "e": 0.2, "i": 0.8, "g": 0.4, "n": 0.5, "c": 0.1}
FAKE_S = sum(FAKE_DIMS.values()) / 6
VECTOR_DIMS = 4


def _sid(tag: str) -> str:
    return f"m15_{tag}_{uuid.uuid4().hex[:6]}"


class FakeScorer:
    def score(self, content: str):
        import json

        return json.dumps(FAKE_DIMS), {"prompt_tokens": 10, "completion_tokens": 5}, FAKE_MODEL


class FakeEmbedder:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[str] = []

    def embed(self, text: str):
        if self.error is not None:
            raise self.error
        self.calls.append(text)
        return [0.1, 0.2, 0.3, 0.4], {"prompt_tokens": 3, "total_tokens": 3}


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
    es = es_client()
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=VECTOR_DIMS)
    pulsar = pulsar_client()
    yield SimpleNamespace(
        store=store,
        cell_session=cell_session,
        ex_session=ex_cluster.connect(),
        gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
        es=es,
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
        gremlin=stack.gremlin,
        es=stack.es,
        ex_session=stack.ex_session,
        redis=stack.redis,
        meta_appender=partial(ex_ingest.append_meta, stack.ex_session),
        mapping_cache=MappingCache(stack.store),
        stream_publisher=ExStreamPublisher(stack.pulsar),
    )


def _claims(space_id: str):
    from lethefield_api.auth import Claims

    return Claims("acct-m15", (space_id,), "claude-code", ("record", "reinforce"))


def _only(space_id: str):
    """writer/SS 的 control_store 收窄到本测试 space（避免消费其他模块滞留消息）。"""
    return SimpleNamespace(list_spaces=lambda: [space_id])


def _writer_deps(stack, space: str, embedder, emitted: list, **config_over) -> WorkerDeps:
    return WorkerDeps(
        gremlin=stack.gremlin,
        es=stack.es,
        ex_session=stack.ex_session,
        embedder=embedder,
        control_store=_only(space),
        quota_counters=QuotaCounters(stack.gremlin, stack.es),
        emit=emitted.append,
        config=WriterConfig(
            receive_timeout_ms=200,
            nack_redelivery_delay_ms=50,
            topic_discovery_seconds=1,
            **config_over,
        ),
    )


def _ss_deps(stack, space: str, scorer) -> SSWorkerDeps:
    return SSWorkerDeps(
        scorer=scorer,
        ex_session=stack.ex_session,
        publisher=ResultPublisher(stack.pulsar),
        control_store=_only(space),
        emit=lambda e: None,
        config=SSConfig(receive_timeout_ms=200, nack_redelivery_delay_ms=50),
    )


def _result(space: str, event_id: str, n: int, s: float = FAKE_S) -> ScoringResult:
    return ScoringResult(
        space_id=space,
        event_id=event_id,
        n=n,
        node_key=node_key_of(event_id),
        dims=dict(FAKE_DIMS),
        s=s,
        model_version=FAKE_MODEL,
        degraded=False,
    )


def _publish_results(stack, space: str, results: list[ScoringResult]) -> None:
    producer = stack.pulsar.create_producer(scoring_results_topic(space))
    try:
        for r in results:
            producer.send(r.to_json().encode("utf-8"))
    finally:
        producer.close()


# ---------------------------------------------------------------- 图断言辅助


def _vertex_count(stack, space: str) -> int:
    return (
        stack.gremlin.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal()\n"
            "t.V().has('space_id', sp).count().next()",
            {"gname": space, "sp": space},
        )
        .all()
        .result()[0]
    )


def _vertex_props(stack, space: str, node_key: str) -> dict | None:
    rows = (
        stack.gremlin.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal()\n"
            "t.V().has('space_id', sp).has('node_key', nk).valueMap().toList()",
            {"gname": space, "sp": space, "nk": node_key},
        )
        .all()
        .result()
    )
    # 服务端按元素逐个流回（非嵌套列表）：result 即 valueMap 的列表
    return rows[0] if rows else None


def _temporal_edges(stack, space: str) -> set[tuple[str, str]]:
    rows = (
        stack.gremlin.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal()\n"
            "t.V().has('space_id', sp).as('a').outE('temporal').inV().as('b')\n"
            "    .select('a', 'b').by('node_key').toList()",
            {"gname": space, "sp": space},
        )
        .all()
        .result()
    )
    return {(r["a"], r["b"]) for r in rows}


def _drain_vertices(stack, space: str, runtime, count: int, timeout_s: float = 90.0) -> None:
    """条件等待：跑 writer 轮直到图内顶点数达标（不赌固定轮数）。"""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        runtime.run_once()
        if _vertex_count(stack, space) >= count:
            return
        time.sleep(0.2)
    raise AssertionError(
        f"超时：{space} 图内顶点未达 {count}（当前 {_vertex_count(stack, space)}）"
    )


def _vector_doc(stack, space: str, node_key: str) -> dict | None:
    resp = stack.es.options(ignore_status=404).get(
        index=VECTORS_INDEX, id=f"{space}:{node_key}", routing=space
    )
    return resp["_source"] if resp.get("found") else None


# ------------------------------------------- 验收 1：端到端字段级


def test_record_to_graph_end_to_end(stack):
    space = _sid("e2e")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="我拿到了心仪公司的 offer！")
    r2 = record(ctx, _claims(space), space_id=space, content="今天午饭吃了碗面。")
    assert (r1["n"], r2["n"]) == (1, 2)

    ss = SSWorkerRuntime(stack.pulsar, _ss_deps(stack, space, FakeScorer()))
    try:
        deadline = time.time() + 60
        while time.time() < deadline:  # 等 SS 产出两条打分结果（EX 回写为准）
            ss.run_once()
            metas = [
                m
                for m in list_meta_events(stack.ex_session, space_id=space)
                if m.meta_type == SCORING_RESULT_META_TYPE
            ]
            if len(metas) >= 2:
                break
            time.sleep(0.2)
    finally:
        ss.close()

    embedder = FakeEmbedder()
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, embedder, []))
    try:
        _drain_vertices(stack, space, runtime, 2)
    finally:
        runtime.close()

    k1, k2 = node_key_of(r1["event_id"]), node_key_of(r2["event_id"])
    e1 = get_experience_event(stack.ex_session, space_id=space, n=1)
    p1 = _vertex_props(stack, space, k1)
    assert p1["node_key"] == [k1]
    assert p1["node_type"] == ["event"]
    assert p1["content"] == ["我拿到了心仪公司的 offer！"]  # c_i 反查 EX
    assert p1["ref_ex"] == [r1["event_id"]]  # 指回 EX 原始事件 ID
    assert p1["s"] == [pytest.approx(FAKE_S)]  # s = SS 合成初值
    assert p1["n_created"] == [1] and p1["n_last_touched"] == [1]  # φ 初始化
    assert p1["reinforce_count"] == [0] and p1["conflict_count"] == [0]
    assert p1["neglect_count"] == [0]
    assert p1["n_star_cached"] == [n_star_horizon(FAKE_S, 1, FF_CONFIG.theta_base)]
    assert p1["agent_actor_id"] == ["claude-code"]  # A_i 来自摄入层盖章列
    # τ_i 与 EX 行一致；EX tau_ms 为空时写入链回退 created_at（naive 按 UTC，M6 踩坑）
    tau_ms = int(p1["tau"][0].replace(tzinfo=UTC).timestamp() * 1000)
    expected_tau = (
        e1.tau_ms
        if e1.tau_ms is not None
        else int(e1.created_at.replace(tzinfo=UTC).timestamp() * 1000)
    )
    assert abs(tau_ms - expected_tau) < 1000

    assert _temporal_edges(stack, space) == {(k1, k2)}  # 时序边 n1 → n2

    doc = _vector_doc(stack, space, k1)
    assert doc["node_key"] == k1 and doc["space_id"] == space  # node_key 关联图顶点
    assert doc["content"] == "我拿到了心仪公司的 offer！"  # Stage 2 关键词字段
    assert len(doc["v"]) == VECTOR_DIMS
    assert set(embedder.calls) == {"我拿到了心仪公司的 offer！", "今天午饭吃了碗面。"}


# ------------------------------------------- 验收 2：幂等（重复投递）


def test_duplicate_delivery_single_vertex_edge_vector(stack):
    space = _sid("idemp")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="幂等验证事件一。")
    r2 = record(ctx, _claims(space), space_id=space, content="幂等验证事件二。")

    # 同一打分结果重复投递 3 次 + 后继事件 1 次（有序订阅：n=2 处理完即证明
    # 前面 3 次重复投递已全部消费）
    _publish_results(
        stack,
        space,
        [_result(space, r1["event_id"], 1)] * 3 + [_result(space, r2["event_id"], 2)],
    )
    embedder = FakeEmbedder()
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, embedder, []))
    try:
        _drain_vertices(stack, space, runtime, 2)
    finally:
        runtime.close()

    k1, k2 = node_key_of(r1["event_id"]), node_key_of(r2["event_id"])
    assert _vertex_count(stack, space) == 2  # 重复投递未重复建点
    assert _temporal_edges(stack, space) == {(k1, k2)}  # 恰好 1 条时序边
    assert _vector_doc(stack, space, k1) is not None
    assert _vector_doc(stack, space, k2) is not None
    assert sorted(embedder.calls) == ["幂等验证事件一。", "幂等验证事件二。"]  # 无重复 embed


# ------------------------------------------- 验收 3：时序边链


def test_temporal_chain_three_events(stack):
    space = _sid("chain")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    rs = [record(ctx, _claims(space), space_id=space, content=f"链式事件 {i}。") for i in (1, 2, 3)]
    _publish_results(stack, space, [_result(space, r["event_id"], r["n"]) for r in rs])
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, FakeEmbedder(), []))
    try:
        _drain_vertices(stack, space, runtime, 3)
    finally:
        runtime.close()
    keys = [node_key_of(r["event_id"]) for r in rs]
    assert _temporal_edges(stack, space) == {(keys[0], keys[1]), (keys[1], keys[2])}


# ------------------------------------------- 验收 4：元事件不建点、不推进 n


def test_reinforce_meta_event_no_vertex(stack):
    space = _sid("meta")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="reinforce 验证事件。")
    _publish_results(stack, space, [_result(space, r1["event_id"], 1)])
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, FakeEmbedder(), []))
    try:
        _drain_vertices(stack, space, runtime, 1)
    finally:
        runtime.close()

    key = node_key_of(r1["event_id"])
    out = reinforce(ctx, _claims(space), space_id=space, node_key=key)
    assert out["applied"] is True
    assert n_now(stack.redis, stack.ex_session, space_id=space) == 1  # 元事件不推进 n

    # reinforce 元事件已在 EX 留痕；writer 再跑数轮——不建任何新顶点
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, FakeEmbedder(), []))
    try:
        for _ in range(3):
            runtime.run_once()
    finally:
        runtime.close()
    assert _vertex_count(stack, space) == 1
    assert _vertex_props(stack, space, key)["reinforce_count"] == [1]  # 旁路 δ 生效


# ------------------------------------------- 验收 5：跨 space 向量隔离


def test_cross_space_vector_isolation(stack):
    space_a, space_b = _sid("iso_a"), _sid("iso_b")
    _provision(stack, space_a)
    _provision(stack, space_b)
    ctx = _api_ctx(stack)
    ra = record(ctx, _claims(space_a), space_id=space_a, content="space A 的私密事件。")
    rb = record(ctx, _claims(space_b), space_id=space_b, content="space B 的私密事件。")
    _publish_results(stack, space_a, [_result(space_a, ra["event_id"], 1)])
    _publish_results(stack, space_b, [_result(space_b, rb["event_id"], 1)])

    for space in (space_a, space_b):
        runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, FakeEmbedder(), []))
        try:
            _drain_vertices(stack, space, runtime, 1)
        finally:
            runtime.close()

    ka, kb = node_key_of(ra["event_id"]), node_key_of(rb["event_id"])
    # kNN 零泄漏双机制（routing + space_id term）：A 的查询只见 A 的文档
    hits_a = knn_search(stack.es, space_id=space_a, query_vector=[0.1, 0.2, 0.3, 0.4], k=10)
    assert {h["node_key"] for h in hits_a} == {ka}
    hits_b = knn_search(stack.es, space_id=space_b, query_vector=[0.1, 0.2, 0.3, 0.4], k=10)
    assert {h["node_key"] for h in hits_b} == {kb}


# ------------------------------------------- 验收 6：n 连续性缺口补偿


def test_n_gap_compensation_from_ex(stack):
    space = _sid("gap")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    # n=1：EX 落库 + scoring_result 已回写，但 scoring-results 发布丢失
    # （模拟"SS 打分完成、发布前崩溃"——writer 消费侧按 n 区间从 EX 补偿建点，
    # s 取 EX details 全保真档，修订记录第 23 条⑤）
    e1_id, n1 = ex_ingest.append_experience(
        stack.ex_session,
        stack.redis,
        space_id=space,
        content="丢失发布的已打分事件。",
        agent_actor_id="claude-code",
        account_id="acct-m15",
    )
    assert n1 == 1
    append_meta_row(
        stack.ex_session,
        space_id=space,
        node_key=node_key_of(e1_id),
        meta_type=SCORING_RESULT_META_TYPE,
        n_at_event=1,
        agent_actor_id="claude-code",
        account_id="acct-m15",
        details=scoring_details_of(
            dims=dict(FAKE_DIMS), s=0.42, model_version=FAKE_MODEL, event_id=e1_id
        ),
    )
    # n=2：正常 record + SS 打分发布（SS 从 EX scoring_result 播种 seed=1 → 无缺口）
    r2 = record(ctx, _claims(space), space_id=space, content="正常打分事件二。")
    ss = SSWorkerRuntime(stack.pulsar, _ss_deps(stack, space, FakeScorer()))
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            ss.run_once()
            scored = [
                m
                for m in list_meta_events(stack.ex_session, space_id=space)
                if m.meta_type == SCORING_RESULT_META_TYPE
            ]
            if len(scored) >= 2:
                break
            time.sleep(0.2)
    finally:
        ss.close()

    emitted: list = []
    runtime = WorkerRuntime(stack.pulsar, _writer_deps(stack, space, FakeEmbedder(), emitted))
    try:
        _drain_vertices(stack, space, runtime, 2)
    finally:
        runtime.close()

    gaps = [e for e in emitted if e.event_type == "writer_n_gap" and e.space_id == space]
    assert gaps and gaps[0].payload["from_n"] == 1 and gaps[0].payload["to_n"] == 1
    k1, k2 = node_key_of(e1_id), node_key_of(r2["event_id"])
    p1 = _vertex_props(stack, space, k1)
    assert p1["s"] == [0.42]  # 补偿建点 s 取 EX details，不取信封
    assert p1["content"] == ["丢失发布的已打分事件。"]
    assert _vertex_props(stack, space, k2) is not None
    assert _temporal_edges(stack, space) == {(k1, k2)}


# ------------------------------------------- 验收 7：嵌入失败 → 重试 → DLQ + 告警


def test_embed_failure_retries_then_dlq(stack):
    space = _sid("dlq")
    _provision(stack, space)
    ctx = _api_ctx(stack)
    r1 = record(ctx, _claims(space), space_id=space, content="DLQ 验证事件。")
    _publish_results(stack, space, [_result(space, r1["event_id"], 1)])

    emitted: list = []
    deps = _writer_deps(
        stack,
        space,
        FakeEmbedder(EmbedError("嵌入超时（fake）")),
        emitted,
        max_redeliver_count=1,
    )
    runtime = WorkerRuntime(stack.pulsar, deps)
    try:
        deadline = time.time() + 60
        while time.time() < deadline:
            runtime.run_once()
            if any(e.event_type == "writer_dlq" for e in emitted):
                break
            time.sleep(0.3)
    finally:
        runtime.close()

    dlq_events = [e for e in emitted if e.event_type == "writer_dlq"]
    assert dlq_events, "重试耗尽应有 page 级 writer_dlq 告警"
    key = node_key_of(r1["event_id"])
    assert _vector_doc(stack, space, key) is None  # 嵌入失败无向量

    # 死信落地（不丢单）：DLQ topic 按命名单点出现且原文可读回
    deadline = time.time() + 30
    dlqs = []
    while time.time() < deadline:
        resp = requests.get(
            f"http://localhost:8080/admin/v2/namespaces/lethefield/{space}/topics", timeout=10
        )
        resp.raise_for_status()
        dlqs = [t for t in resp.json() if t.rstrip("/").endswith("-DLQ")]
        if dlqs:
            break
        time.sleep(0.5)
    assert dlqs == [scoring_results_dlq_topic(space)], "DLQ topic 应按命名单点出现"
    reader = stack.pulsar.create_reader(dlqs[0], start_message_id=pulsar.MessageId.earliest)
    try:
        msg = reader.read_next(timeout_millis=10000)
        assert ScoringResult.from_json(msg.data().decode("utf-8")).event_id == r1["event_id"]
    finally:
        reader.close()
