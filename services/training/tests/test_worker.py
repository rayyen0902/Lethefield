"""worker 消息处理单测（fake 依赖注入，不起栈）。"""

from lethefield_clients import FeedEvent, FeedKind, FeedSource, SpaceDestroyCommand
from lethefield_logschema import LogEvent
from lethefield_training import worker
from lethefield_training.config import TrainingConfig
from lethefield_training.hot_store import HotSampleStore
from lethefield_training.recall_window import RecallWindow


class FakeRegistry:
    def __init__(self, authorized=True):
        self.authorized = authorized
        self.deleted: list[str] = []

    def is_authorized(self, space_ref, scope):
        return self.authorized

    def delete(self, space_ref):
        self.deleted.append(space_ref)
        return True


def _deps(tmp_path, authorized=True):
    emitted: list[LogEvent] = []
    deps = worker.WorkerDeps(
        store=HotSampleStore(tmp_path / "hot"),
        window=RecallWindow(tmp_path / "w.jsonl", w_r3_ms=60_000),
        registry=FakeRegistry(authorized),
        emit=emitted.append,
        config=TrainingConfig(),
    )
    return deps, emitted


def _feed(kind, source, space_ref, payload) -> FeedEvent:
    return FeedEvent(kind=kind, source=source, space_ref=space_ref, payload=payload)


# ---------------------------------------------------------------- ③ 召回明细


def test_recall_detail_authorized_records_window_no_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(
        _feed(
            FeedKind.RECALL_DETAIL,
            FeedSource.FF_METRIC,
            "ref_a",
            {
                "event_id": "evt-1",
                "node_keys": ["ev_1"],
                "theta": {"anchors": 1},
                "query_class": "vector",
            },
        ),
        deps,
    )
    assert deps.window.recalled_at("ref_a", "ev_1") is not None
    assert deps.store.manifest("ref_a") == []  # 召回明细过境不产样本


def test_recall_detail_unauthorized_dropped(tmp_path):
    deps, emitted = _deps(tmp_path, authorized=False)
    worker.process_feed_event(
        _feed(
            FeedKind.RECALL_DETAIL,
            FeedSource.FF_METRIC,
            "ref_a",
            {"event_id": "evt-unauth", "node_keys": ["ev_1"]},
        ),
        deps,
    )
    assert deps.window.recalled_at("ref_a", "ev_1") is None  # 第二道防线：未授权丢弃
    assert any(e.event_type == "training_feed_dropped" for e in emitted)


# ---------------------------------------------------------------- R3 关联


def _correction(space_ref="ref_a", old="ev_1"):
    return _feed(
        FeedKind.CORRECTION_PAIR,
        FeedSource.EX_DERIVED,
        space_ref,
        {
            "old_node_key": old,
            "new_node_key": "ev_2",
            "before": "旧内容",
            "after": "新内容",
            "corrected_at": "2026-08-08T00:00:00+00:00",
            "n": 2,
        },
    )


def test_r3_hit_produces_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(
        _feed(
            FeedKind.RECALL_DETAIL,
            FeedSource.FF_METRIC,
            "ref_a",
            {"event_id": "evt-unauth", "node_keys": ["ev_1"]},
        ),
        deps,
    )
    worker.process_feed_event(_correction(), deps)
    manifest = deps.store.manifest("ref_a")
    assert len(manifest) == 1
    sample = deps.store.load_sample(manifest[0].file, manifest[0].sample_id)
    assert sample.rule == "R3"
    assert sample.source == "ex_derived"
    assert sample.auth_scope == "granted"
    assert sample.diagnosis == {"before": "旧内容", "after": "新内容"}
    assert sample.problem["recalled_node_key"] == "ev_1"


def test_r3_miss_without_recall(tmp_path):
    deps, emitted = _deps(tmp_path)
    worker.process_feed_event(_correction(), deps)  # 未经召回的纠错
    assert deps.store.manifest("ref_a") == []
    assert emitted[-1].payload["reason"] == "r3_miss"


def test_r3_miss_across_space(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(
        _feed(
            FeedKind.RECALL_DETAIL,
            FeedSource.FF_METRIC,
            "ref_b",
            {"event_id": "evt-b", "node_keys": ["ev_1"]},
        ),
        deps,
    )
    worker.process_feed_event(_correction(space_ref="ref_a"), deps)
    assert deps.store.manifest("ref_a") == []  # 召回在 ref_b，纠错在 ref_a，不关联


# ---------------------------------------------------------------- ① R1/R2


def _decision(outcome="accepted", escalation=None):
    return _feed(
        FeedKind.DECISION_COMPARISON,
        FeedSource.DECISION_LOG,
        None,
        {
            "record_id": 1,
            "title": "扩容决策",
            "context": "ctx",
            "agent_suggestion": "建议 A",
            "decision": "采用 B",
            "outcome": outcome,
            "rationale": "r",
            "decided_by": "op-1",
            "escalation_type": escalation,
        },
    )


def test_r1_rejected_produces_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(_decision(outcome="rejected"), deps)
    manifest = deps.store.manifest(None)
    assert len(manifest) == 1
    sample = deps.store.load_sample(manifest[0].file, manifest[0].sample_id)
    assert sample.rule == "R1"
    assert sample.auth_scope == "ops_only"
    assert sample.diagnosis == {"agent_suggestion": "建议 A"}


def test_r2_escalation_produces_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(_decision(escalation="novel_error"), deps)
    assert [e.rule for e in deps.store.manifest(None)] == ["R2"]


def test_r1_r2_both_hit_two_samples(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(_decision(outcome="modified", escalation="cross_space"), deps)
    assert sorted(e.rule for e in deps.store.manifest(None)) == ["R1", "R2"]


def test_accepted_no_escalation_no_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(_decision(), deps)
    assert deps.store.manifest(None) == []  # 常规流量不产样本


# ---------------------------------------------------------------- ② 故障案例


def test_incident_produces_r5_sample(tmp_path):
    deps, _ = _deps(tmp_path)
    worker.process_feed_event(
        _feed(
            FeedKind.INCIDENT,
            FeedSource.INCIDENT,
            None,
            {
                "problem": {"text": "p"},
                "diagnosis": {"text": "d"},
                "decision": {"text": "c"},
                "outcome": {"text": "o"},
            },
        ),
        deps,
    )
    manifest = deps.store.manifest(None)
    assert len(manifest) == 1
    sample = deps.store.load_sample(manifest[0].file, manifest[0].sample_id)
    assert sample.rule == "R5"
    assert sample.problem == {"text": "p"}


# ---------------------------------------------------------------- 契约 5 销毁处置


def test_destroy_scrubs_deletes_registry_and_logs(tmp_path):
    deps, emitted = _deps(tmp_path)
    deps.store.append(
        [
            worker.TrainingSample.new(
                source="ex_derived",
                rule="R3",
                space_ref="ref_a",
                problem={"c": "secret"},
                diagnosis={},
                decision={},
                outcome={},
                auth_scope="granted",
            )
        ]
    )
    command = SpaceDestroyCommand(space_ref="ref_a", initiator="ops", ticket_ref="t-1")
    worker.process_control_message(command.to_json().encode(), deps)
    entry = deps.store.manifest("ref_a")[0]
    assert entry.scrubbed is True
    assert deps.store.load_sample(entry.file, entry.sample_id).problem == {}
    assert deps.registry.deleted == ["ref_a"]
    event = emitted[-1]
    assert event.event_type == "training_space_destroy_processed"
    assert event.payload["scrubbed_count"] == 1
    assert event.payload["registry_entry_deleted"] is True
    assert event.payload["ticket_ref"] == "t-1"


# ---------------------------------------------------------------- M12：event_id 去重


def test_recall_detail_missing_event_id_fail_closed(tmp_path):
    import pytest

    deps, _ = _deps(tmp_path)
    with pytest.raises(ValueError, match="event_id"):
        worker.process_feed_event(
            _feed(FeedKind.RECALL_DETAIL, FeedSource.FF_METRIC, "ref_a", {"node_keys": ["ev_1"]}),
            deps,
        )


def test_recall_detail_duplicate_event_id_deduped(tmp_path):
    deps, _ = _deps(tmp_path)
    event = _feed(
        FeedKind.RECALL_DETAIL,
        FeedSource.FF_METRIC,
        "ref_a",
        {"event_id": "evt-dup", "node_keys": ["ev_1"]},
    )
    worker.process_feed_event(event, deps)
    first_seen = deps.window.recalled_at("ref_a", "ev_1")
    assert first_seen is not None
    # at-least-once 重放：同 event_id 不重复登记（窗口时间戳不被刷新）
    worker.process_feed_event(event, deps)
    assert deps.window.recalled_at("ref_a", "ev_1") == first_seen
