"""样本 schema 单测。"""

import pytest
from lethefield_training.sample import TrainingSample


def _sample(**kwargs) -> TrainingSample:
    defaults = {
        "source": "decision_log",
        "rule": "R1",
        "space_ref": None,
        "problem": {"title": "t"},
        "diagnosis": {"agent_suggestion": "s"},
        "decision": {"decision": "d"},
        "outcome": {"outcome": "rejected"},
        "auth_scope": "ops_only",
    }
    return TrainingSample.new(**(defaults | kwargs))


def test_serde_roundtrip():
    sample = _sample()
    restored = TrainingSample.from_json(sample.to_json())
    assert restored == sample
    assert restored.review["status"] == "pending"
    assert restored.created_at.tzinfo is not None


def test_from_json_rejects_bad_version():
    import json

    obj = json.loads(_sample().to_json())
    obj["v"] = 2
    with pytest.raises(ValueError, match="版本不符"):
        TrainingSample.from_json(json.dumps(obj))


def test_from_json_rejects_bad_auth_scope():
    import json

    obj = json.loads(_sample().to_json())
    obj["auth_scope"] = "everything"
    with pytest.raises(ValueError, match="auth_scope"):
        TrainingSample.from_json(json.dumps(obj))


def test_scrubbed_copy_clears_content_keeps_skeleton():
    sample = _sample(space_ref="a" * 64)
    scrubbed = sample.scrubbed_copy()
    assert scrubbed.scrubbed is True
    for field in ("problem", "diagnosis", "decision", "outcome"):
        assert getattr(scrubbed, field) == {}
    # 骨架保留：sample_id/source/rule/space_ref/auth_scope/review/created_at
    assert scrubbed.sample_id == sample.sample_id
    assert scrubbed.rule == "R1"
    assert scrubbed.space_ref == sample.space_ref
    assert scrubbed.review == sample.review
