"""ex_stream 单测（M14）：topic 命名、space 解析、两信封 fail-closed 序列化。"""

import pytest
from lethefield_clients.ex_stream import (
    DIMENSIONS,
    ExStreamEvent,
    ScoringResult,
    ex_events_topic,
    scoring_results_topic,
    space_id_of_topic,
)


def test_topic_naming():
    assert ex_events_topic("demo") == "persistent://lethefield/demo/ex-events"
    assert scoring_results_topic("demo") == "persistent://lethefield/demo/scoring-results"


def test_topic_naming_fail_closed():
    with pytest.raises(ValueError, match="命名约束"):
        ex_events_topic("Bad-Space")


def test_space_id_of_topic():
    assert space_id_of_topic("persistent://lethefield/demo/ex-events") == "demo"


@pytest.mark.parametrize(
    "bad",
    [
        "persistent://other/demo/ex-events",  # 非业务 tenant
        "persistent://lethefield/demo",  # 缺段
        "non-persistent://lethefield/demo/ex-events/x",
    ],
)
def test_space_id_of_topic_fail_closed(bad):
    with pytest.raises(ValueError, match="topic 形式不符"):
        space_id_of_topic(bad)


def _ex_event() -> ExStreamEvent:
    return ExStreamEvent(
        space_id="demo",
        event_id="e1",
        n=3,
        content="内容",
        agent_actor_id="agent",
        account_id="acc",
        tau_ms=None,
        ref_conflict=None,
        created_at_ms=1720000000000,
    )


def test_ex_stream_event_roundtrip():
    event = _ex_event()
    assert ExStreamEvent.from_json(event.to_json()) == event


def test_ex_stream_event_version_fail_closed():
    data = _ex_event().to_json().replace('"v": 1', '"v": 99')
    with pytest.raises(ValueError, match="版本不符"):
        ExStreamEvent.from_json(data)


def test_ex_stream_event_missing_field_fail_closed():
    data = _ex_event().to_json().replace('"n": 3,', "")
    with pytest.raises(ValueError, match="缺字段"):
        ExStreamEvent.from_json(data)


def _scoring() -> ScoringResult:
    return ScoringResult(
        space_id="demo",
        event_id="e1",
        n=3,
        node_key="ev_e1",
        dims={d: 0.5 for d in DIMENSIONS},
        s=0.5,
        model_version="m1",
        degraded=False,
    )


def test_scoring_result_roundtrip():
    result = _scoring()
    assert ScoringResult.from_json(result.to_json()) == result


def test_scoring_result_degraded_roundtrip():
    result = ScoringResult(
        **{**_scoring().__dict__, "degraded": True, "missing_dims": ["er"], "scored_at_ms": 1}
    )
    parsed = ScoringResult.from_json(result.to_json())
    assert parsed.degraded is True
    assert parsed.missing_dims == ["er"]
    assert parsed.scored_at_ms == 1


def test_scoring_result_dims_key_mismatch_fail_closed():
    result = _scoring()
    bad = result.to_json().replace('"er"', '"xx"')
    with pytest.raises(ValueError, match="维度键不符"):
        ScoringResult.from_json(bad)


def test_scoring_result_version_fail_closed():
    data = _scoring().to_json().replace('"v": 1', '"v": 0')
    with pytest.raises(ValueError, match="版本不符"):
        ScoringResult.from_json(data)
