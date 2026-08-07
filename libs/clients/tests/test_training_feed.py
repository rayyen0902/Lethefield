"""M11 训练 feed 信封与判定规则单测。"""

import pytest
from lethefield_clients import (
    DECISION_OUTCOMES,
    ESCALATION_TYPES,
    RULE_R1,
    RULE_R2,
    FeedEvent,
    FeedKind,
    FeedSource,
    decision_rules,
    feed_topic,
)


def _event() -> FeedEvent:
    return FeedEvent(
        kind=FeedKind.RECALL_DETAIL,
        source=FeedSource.FF_METRIC,
        space_ref="a" * 64,
        payload={"node_keys": ["ev_1"], "theta": {"anchors": 1}},
    )


def test_feed_topic_fqn():
    assert feed_topic() == "persistent://lethefield-training/feeds/raw"


def test_serde_roundtrip():
    event = _event()
    restored = FeedEvent.from_json(event.to_json())
    assert restored == event
    assert restored.emitted_at.tzinfo is not None


def test_from_json_rejects_bad_version():
    event = _event()
    import json

    obj = json.loads(event.to_json())
    obj["v"] = 99
    with pytest.raises(ValueError, match="版本不符"):
        FeedEvent.from_json(json.dumps(obj))


def test_from_json_rejects_unknown_kind():
    import json

    obj = json.loads(_event().to_json())
    obj["kind"] = "query_log"
    with pytest.raises(ValueError, match="未知 feed"):
        FeedEvent.from_json(json.dumps(obj))


def test_from_json_rejects_kind_source_mismatch():
    import json

    obj = json.loads(_event().to_json())
    obj["source"] = "incident"
    with pytest.raises(ValueError, match="不配对"):
        FeedEvent.from_json(json.dumps(obj))


def test_decision_rules():
    assert decision_rules("accepted", None) == []
    assert decision_rules("rejected", None) == [RULE_R1]
    assert decision_rules("modified", None) == [RULE_R1]
    assert decision_rules("accepted", "cross_space") == [RULE_R2]
    assert decision_rules("rejected", "low_confidence") == [RULE_R1, RULE_R2]


def test_decision_constants_cover_design():
    # M0 任务 5 定案：outcome 三值 + §11.2 升级四类
    assert frozenset({"accepted", "modified", "rejected"}) == DECISION_OUTCOMES
    assert (
        frozenset({"ex_write_path", "cross_space", "novel_error", "low_confidence"})
        == ESCALATION_TYPES
    )
