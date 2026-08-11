"""recall_filter 单测（fake es_ops/registry/publish）。"""

import pytest
from lethefield_clients import FeedKind
from lethefield_training import recall_filter


class FakeEs:
    def __init__(self, pages):
        self.pages = pages  # list of (hits, ) 每页返回一批

    def search(self, index, body):
        page = self.pages.pop(0) if self.pages else []
        return {"hits": {"hits": [{"_source": e, "sort": [e["_ts"]]} for e in page]}}


def _recall(space_ref, event_id, ts="2026-08-11T10:00:00+00:00", ts_ms=1_800_000_000_000):
    return {
        "_ts": ts_ms,
        "timestamp": ts,
        "event_type": "retrieve_recall_detail",
        "space_id": "sp_x",
        "payload": {
            "event_id": event_id,
            "space_ref": space_ref,
            "node_keys": ["ev_1"],
            "theta": {},
            "query_class": "vector",
            "stage_ms": {"knn": 1.0},
        },
    }


class FakeRegistry:
    def __init__(self, authorized):
        self.authorized = authorized

    def is_authorized(self, space_ref, scope):
        return self.authorized


def test_authorized_forwarded_and_cursor_saved(tmp_path):
    es = FakeEs([[_recall("ref_a", "e1")]])
    published = []
    state = tmp_path / "state.json"
    n = recall_filter.run_once(
        es, registry=FakeRegistry(True), publish=published.append, state_path=state
    )
    assert n == 1
    assert published[0].kind is FeedKind.RECALL_DETAIL
    assert published[0].payload["event_id"] == "e1"
    # 原始时间戳保留（W_r3 窗关联依赖真实召回时刻）
    assert published[0].emitted_at.year == 2026
    # checkpoint 推进：重跑不重复转发
    n2 = recall_filter.run_once(
        es, registry=FakeRegistry(True), publish=published.append, state_path=state
    )
    assert n2 == 0 and len(published) == 1


def test_unauthorized_intercepted_before_topic(tmp_path):
    es = FakeEs([[_recall("ref_a", "e1")]])
    published = []
    n = recall_filter.run_once(
        es,
        registry=FakeRegistry(False),
        publish=published.append,
        state_path=tmp_path / "state.json",
    )
    assert n == 0 and published == []


def test_missing_event_id_fail_closed(tmp_path):
    bad = _recall("ref_a", "e1")
    bad["payload"]["event_id"] = ""
    es = FakeEs([[bad]])
    with pytest.raises(ValueError, match="event_id"):
        recall_filter.run_once(
            es,
            registry=FakeRegistry(True),
            publish=lambda e: None,
            state_path=tmp_path / "state.json",
        )
