from datetime import UTC, datetime

import pytest
from lethefield_logschema import LogEvent
from pydantic import ValidationError


def test_jsonl_roundtrip():
    event = LogEvent(
        service="rms",
        event_type="retrieve_detail",
        space_id="space-123",
        payload={"s_effective": 0.42, "filtered": True},
    )
    restored = LogEvent.from_jsonl(event.to_jsonl())
    assert restored == event
    assert restored.timestamp.tzinfo is not None


def test_default_timestamp_is_utc_now():
    before = datetime.now(UTC)
    event = LogEvent(service="ex", event_type="ingest")
    after = datetime.now(UTC)
    assert before <= event.timestamp <= after


def test_space_id_optional_for_service_level_events():
    event = LogEvent(service="fs", event_type="sweep_started")
    assert event.space_id is None


def test_missing_required_field_rejected():
    with pytest.raises(ValidationError):
        LogEvent.model_validate({"service": "rms"})


def test_extra_field_rejected():
    with pytest.raises(ValidationError):
        LogEvent.model_validate({"service": "rms", "event_type": "x", "unexpected": 1})
