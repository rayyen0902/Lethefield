"""M11 训练数据管线集成测试（真实 Pulsar + PostgreSQL + EX，默认栈）。

覆盖 M11 验收：
1. 未授权 space 的 ③④ 类数据在入 topic 前被拒（授权状态切换前后对比）。
2. R1–R3 各有可触发场景，命中产符合 schema 的样本；常规流量不产样本。
3. 撤回授权 → 存量样本按 space_ref 清单 O(清单) 定位、内容清除、骨架保留。
4. M10 销毁广播 → worker 实际消费（training-destroy-sink）→ 等效处置 +
   注册表项删除 + 处置动作进决策留痕。
"""

import time
import uuid
from types import SimpleNamespace

import pytest
from lethefield_api.ex_ingest import append_experience
from lethefield_clients import (
    CONTROL_NAMESPACE,
    FEEDS_NAMESPACE,
    RULE_R1,
    RULE_R2,
    RULE_R3,
    RULE_R5,
    TRAINING_TENANT,
    AuthRegistryStore,
    AuthScope,
    FeedEvent,
    FeedKind,
    FeedSource,
    ensure_ex_keyspace,
    es_client,
    ex_cassandra_cluster,
    keyspace_name,
    make_feed_publisher,
    pulsar_client,
    redis_client,
    space_ref_of,
)
from lethefield_decision_log import DecisionLogStore
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as logschema_emit
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import SchedulerConfig
from lethefield_scheduler.destroy_broadcast import make_broadcast
from lethefield_training import recall_filter, worker
from lethefield_training.config import TrainingConfig
from lethefield_training.hot_store import HotSampleStore
from lethefield_training.recall_window import RecallWindow
from lethefield_training.sample import TrainingSample


def _sid(tag: str) -> str:
    return f"m11_{tag}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    config = SchedulerConfig()
    # 契约 5 控制 namespace（reset 后是全新 Pulsar 卷，本模块自建；同 M10 fixture）
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE)
    pulsar_admin.set_retention(
        config.pulsar_admin_url,
        TRAINING_TENANT,
        CONTROL_NAMESPACE,
        minutes=config.training_control_retention_minutes,
        size_mb=-1,
    )
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, FEEDS_NAMESPACE)
    pulsar_admin.set_retention(
        config.pulsar_admin_url,
        TRAINING_TENANT,
        FEEDS_NAMESPACE,
        minutes=config.training_feeds_retention_minutes,
        size_mb=-1,
    )
    pulsar = pulsar_client()
    ex_cluster = ex_cassandra_cluster()
    emitted: list[LogEvent] = []
    hot_root = tmp_path_factory.mktemp("training_hot")
    deps = worker.WorkerDeps(
        store=HotSampleStore(hot_root),
        window=RecallWindow(hot_root / "recall_window.jsonl", w_r3_ms=86_400_000),
        registry=AuthRegistryStore(),
        emit=emitted.append,
        config=TrainingConfig(),
    )
    runtime = worker.WorkerRuntime(pulsar, deps)
    yield SimpleNamespace(
        pulsar=pulsar,
        config=config,
        publish=make_feed_publisher(pulsar),
        registry=deps.registry,
        decision_log=DecisionLogStore(),
        hot_root=hot_root,
        ex_session=ex_cluster.connect(),
        redis=redis_client(),
        runtime=runtime,
        emitted=emitted,
        # es-ops 日志集群（M12 日志管线；shipper 默认 URL 同一约定）
        es_ops=es_client("http://localhost:9201"),
    )
    runtime.close()
    pulsar.close()
    ex_cluster.shutdown()


def _drain(stack) -> None:
    """排空两个 topic 的残留消息 + 清留痕缓冲（durable 订阅跨用例共享，前置排水保隔离）。"""
    stack.runtime.run_once(timeout_ms=500)
    stack.emitted.clear()


def _samples_of(store: HotSampleStore, space_ref: str | None) -> list[TrainingSample]:
    return [store.load_sample(e.file, e.sample_id) for e in store.manifest(space_ref)]


# ------------------------------------------------- ③ 授权闸门（入 topic 前，M12 定案形态）


def _emit_recall_log(space_id: str, space_ref: str, node_keys: list[str]) -> None:
    """模拟 API retrieve 的召回明细发射（M12：API 只发日志事件进 es-ops，不直发 topic）。"""
    logschema_emit(
        LogEvent(
            service="lethefield-api",
            event_type="retrieve_recall_detail",
            space_id=space_id,
            payload={
                "event_id": uuid.uuid4().hex,
                "space_ref": space_ref,
                "node_keys": node_keys,
                "theta": {"anchors": 1, "pool": 1, "returned": 1},
                "query_class": "vector",
                "stage_ms": {"knn": 1.0, "subgraph": 2.0, "ff_filter": 0.5},
            },
        ),
        sync=True,
    )


def _wait_event_in_ops(es_ops, space_id: str, timeout_s: float = 15.0) -> None:
    """等 es-ops 刷新可见（sync bulk 后 refresh 有近秒级延迟）。"""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        es_ops.indices.refresh(index="lethefield-logs-*")
        resp = es_ops.search(
            index="lethefield-logs-*",
            body={
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"event_type": "retrieve_recall_detail"}},
                            {"term": {"space_id": space_id}},
                        ]
                    }
                }
            },
        )
        if resp["hits"]["hits"]:
            return
        time.sleep(0.5)
    raise TimeoutError(f"召回明细事件未在 es-ops 出现：{space_id}")


def test_recall_filter_gate(stack):
    """③ 定案形态（M12）：日志管线 → recall_filter 授权闸门 → 训练 topic。

    未授权 → 拦截（入 topic 前）；grant → 转发（event_id 保留）；revoke → 再不转发。
    """
    space_id = _sid("gate")
    space_ref = space_ref_of(space_id)
    published: list[FeedEvent] = []
    state_path = stack.hot_root / "filter_state" / f"{space_id}.json"

    _emit_recall_log(space_id, space_ref, ["ev_gate1"])
    _wait_event_in_ops(stack.es_ops, space_id)
    assert (
        recall_filter.run_once(
            stack.es_ops,
            registry=stack.registry,
            publish=published.append,
            state_path=state_path,
        )
        == 0
    )
    assert published == []  # 未授权：入 topic 前拦截

    stack.registry.grant(space_ref, [AuthScope.CALIBRATION])
    _emit_recall_log(space_id, space_ref, ["ev_gate2"])
    _wait_event_in_ops(stack.es_ops, space_id)
    assert (
        recall_filter.run_once(
            stack.es_ops,
            registry=stack.registry,
            publish=published.append,
            state_path=state_path,
        )
        == 1  # 只转发新增授权后的事件（首轮被拦截的条目 checkpoint 已过，不回溯）
    )
    event = published[-1]
    assert event.kind is FeedKind.RECALL_DETAIL
    assert event.space_ref == space_ref
    # 字段最小化 + M12 增补键：无 query 原文/内容摘要
    assert set(event.payload) == {
        "event_id",
        "space_ref",
        "node_keys",
        "theta",
        "query_class",
        "stage_ms",
    }
    assert event.payload["query_class"] == "vector"

    stack.registry.revoke(space_ref)
    _emit_recall_log(space_id, space_ref, ["ev_gate3"])
    _wait_event_in_ops(stack.es_ops, space_id)
    recall_filter.run_once(
        stack.es_ops,
        registry=stack.registry,
        publish=published.append,
        state_path=state_path,
    )
    assert all(p.payload["node_keys"] != ["ev_gate3"] for p in published)  # 撤回后再不转发


# ---------------------------------------------------------------- ① R1/R2


def test_r1_r2_from_decision_submit(stack):
    _drain(stack)
    store = DecisionLogStore(publish=stack.publish)
    marker = uuid.uuid4().hex[:8]

    accepted_id = store.submit(
        title=f"M11 常规决策 {marker}", decision="A", decided_by="ci", outcome="accepted"
    )
    rejected_id = store.submit(
        title=f"M11 否决决策 {marker}",
        decision="B",
        decided_by="ci",
        agent_suggestion="建议 A",
        outcome="rejected",
    )
    escalated_id = store.submit(
        title=f"M11 升级决策 {marker}",
        decision="C",
        decided_by="ci",
        outcome="accepted",
        escalation_type="cross_space",
    )
    assert accepted_id and rejected_id and escalated_id

    stack.runtime.run_once()
    samples = _samples_of(HotSampleStore(stack.hot_root), None)
    by_title = {s.problem.get("title"): s for s in samples}
    assert f"M11 常规决策 {marker}" not in by_title  # 常规流量不产样本
    assert by_title[f"M11 否决决策 {marker}"].rule == RULE_R1
    assert by_title[f"M11 否决决策 {marker}"].diagnosis == {"agent_suggestion": "建议 A"}
    assert by_title[f"M11 升级决策 {marker}"].rule == RULE_R2
    # 样本 schema 完整
    sample = by_title[f"M11 否决决策 {marker}"]
    assert sample.sample_id and sample.review["status"] == "pending"
    assert sample.auth_scope == "ops_only" and sample.space_ref is None


# ---------------------------------------------------------------- ② 故障案例


def test_incident_becomes_r5_sample(stack):
    _drain(stack)
    marker = uuid.uuid4().hex[:8]
    stack.publish(
        FeedEvent(
            kind=FeedKind.INCIDENT,
            source=FeedSource.INCIDENT,
            space_ref=None,
            payload={
                "problem": {"text": f"演练故障 {marker}"},
                "diagnosis": {"text": "d"},
                "decision": {"text": "c"},
                "outcome": {"text": "o"},
            },
        )
    )
    stack.runtime.run_once()
    samples = _samples_of(HotSampleStore(stack.hot_root), None)
    hit = [s for s in samples if s.problem.get("text") == f"演练故障 {marker}"]
    assert len(hit) == 1 and hit[0].rule == RULE_R5


# ---------------------------------------------------------------- ③×④ R3 关联


def test_r3_correlation_and_miss(stack):
    _drain(stack)
    space_id = _sid("r3")
    space_ref = space_ref_of(space_id)
    stack.registry.grant(space_ref, [AuthScope.CALIBRATION, AuthScope.CONTENT_COPY])
    recalled_key = f"ev_{uuid.uuid4().hex[:8]}"
    stray_key = f"ev_{uuid.uuid4().hex[:8]}"

    def correction(old_key: str) -> FeedEvent:
        return FeedEvent(
            kind=FeedKind.CORRECTION_PAIR,
            source=FeedSource.EX_DERIVED,
            space_ref=space_ref,
            payload={
                "old_node_key": old_key,
                "new_node_key": f"ev_{uuid.uuid4().hex[:8]}",
                "before": "旧",
                "after": "新",
                "corrected_at": "2026-08-08T00:00:00+00:00",
                "n": 2,
            },
        )

    # 未经召回的纠错：不计入 R3（语义边界）
    stack.publish(correction(stray_key))
    # 召回明细 → 窗内纠错：命中
    stack.publish(
        FeedEvent(
            kind=FeedKind.RECALL_DETAIL,
            source=FeedSource.FF_METRIC,
            space_ref=space_ref,
            payload={
                "event_id": uuid.uuid4().hex,
                "space_ref": space_ref,
                "node_keys": [recalled_key],
                "theta": {},
                "query_class": "hybrid",
            },
        )
    )
    stack.publish(correction(recalled_key))
    stack.runtime.run_once()

    samples = _samples_of(HotSampleStore(stack.hot_root), space_ref)
    assert len(samples) == 1
    sample = samples[0]
    assert sample.rule == RULE_R3
    assert sample.problem["recalled_node_key"] == recalled_key
    assert sample.diagnosis == {"before": "旧", "after": "新"}
    assert sample.auth_scope == "granted"
    assert any(
        e.event_type == "training_feed_dropped" and e.payload["reason"] == "r3_miss"
        for e in stack.emitted
    )


def test_worker_drops_unauthorized_feed(stack):
    """worker 侧第二道防线：未授权 space 的 ③④ 消息即使进了 topic 也丢弃。"""
    _drain(stack)
    space_ref = space_ref_of(_sid("noauth"))
    stack.publish(
        FeedEvent(
            kind=FeedKind.RECALL_DETAIL,
            source=FeedSource.FF_METRIC,
            space_ref=space_ref,
            payload={
                "event_id": uuid.uuid4().hex,
                "space_ref": space_ref,
                "node_keys": ["ev_x"],
                "theta": {},
                "query_class": "keyword",
            },
        )
    )
    stack.runtime.run_once()
    assert HotSampleStore(stack.hot_root).manifest(space_ref) == []
    assert any(
        e.event_type == "training_feed_dropped" and e.payload["reason"] == "unauthorized"
        for e in stack.emitted
    )


# ---------------------------------------------------------------- ④ EX 只读派生


def test_ex_feed_from_real_ex(stack):
    _drain(stack)
    space_id = _sid("exfeed")
    space_ref = space_ref_of(space_id)
    ensure_ex_keyspace(stack.ex_session, space_id)
    try:
        event_id, _ = append_experience(
            stack.ex_session,
            stack.redis,
            space_id=space_id,
            content="原始记忆",
            agent_actor_id="ci",
            account_id="ci",
        )
        append_experience(
            stack.ex_session,
            stack.redis,
            space_id=space_id,
            content="纠正后记忆",
            agent_actor_id="ci",
            account_id="ci",
            ref_conflict=f"ev_{event_id}",
        )
        # 未授权：入 topic 前拒发
        from lethefield_training import ex_feed

        state_path = stack.hot_root / "ex_state" / f"{space_id}.json"
        with pytest.raises(PermissionError):
            ex_feed.run(
                stack.ex_session,
                space_id=space_id,
                registry=stack.registry,
                publish=stack.publish,
                state_path=state_path,
            )
        # 授权后：纠错对发布 → worker 无召回窗命中，按 r3_miss 丢弃（不计入 R3）
        stack.registry.grant(space_ref, [AuthScope.CONTENT_COPY])
        assert (
            ex_feed.run(
                stack.ex_session,
                space_id=space_id,
                registry=stack.registry,
                publish=stack.publish,
                state_path=state_path,
            )
            == 1
        )
        stack.runtime.run_once()
        assert HotSampleStore(stack.hot_root).manifest(space_ref) == []
        assert any(e.payload.get("reason") == "r3_miss" for e in stack.emitted)
    finally:
        stack.ex_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name(space_id)}")


# ---------------------------------------------------------------- 撤回 / 销毁联动


def test_revoke_then_scrub_via_manifest(stack):
    store = HotSampleStore(stack.hot_root)
    ref_a, ref_b = space_ref_of(_sid("rsa")), space_ref_of(_sid("rsb"))
    store.append(
        [
            TrainingSample.new(
                source="ex_derived",
                rule="R3",
                space_ref=ref_a,
                problem={"c": "秘密a"},
                diagnosis={},
                decision={},
                outcome={},
                auth_scope="granted",
            ),
            TrainingSample.new(
                source="ex_derived",
                rule="R3",
                space_ref=ref_b,
                problem={"c": "秘密b"},
                diagnosis={},
                decision={},
                outcome={},
                auth_scope="granted",
            ),
        ]
    )
    stack.registry.grant(ref_a, [AuthScope.CONTENT_COPY])
    stack.registry.revoke(ref_a)  # 撤回授权 → 停止新增（闸门语义前序用例已验）
    assert store.scrub(ref_a) == 1  # O(清单) 定位 → 内容清除
    sample_a = _samples_of(store, ref_a)[0]
    assert sample_a.scrubbed is True and sample_a.problem == {}
    assert sample_a.sample_id and sample_a.rule == "R3"  # 骨架保留
    assert _samples_of(store, ref_b)[0].problem == {"c": "秘密b"}  # 他 space 不受影响


def test_destroy_command_consumed_and_processed(stack):
    """M10 销毁广播 → worker 真实消费（training-destroy-sink）→ 等效处置 + 留痕。"""
    _drain(stack)
    space_id = _sid("dest")
    space_ref = space_ref_of(space_id)
    store = HotSampleStore(stack.hot_root)
    store.append(
        [
            TrainingSample.new(
                source="ex_derived",
                rule="R3",
                space_ref=space_ref,
                problem={"c": "待销毁"},
                diagnosis={},
                decision={},
                outcome={},
                auth_scope="granted",
            )
        ]
    )
    stack.registry.grant(space_ref, [AuthScope.CONTENT_COPY])
    assert stack.registry.get(space_ref) is not None

    make_broadcast(stack.pulsar, initiator="m11-itest")(space_id)  # 真实广播等 broker ack
    stack.runtime.run_once()

    sample = _samples_of(store, space_ref)[0]
    assert sample.scrubbed is True and sample.problem == {}
    assert stack.registry.get(space_ref) is None  # 注册表项删除
    processed = [e for e in stack.emitted if e.event_type == "training_space_destroy_processed"]
    assert len(processed) == 1
    assert processed[0].payload["space_ref"] == space_ref
    assert processed[0].payload["scrubbed_count"] == 1
    assert processed[0].payload["initiator"] == "m11-itest"
