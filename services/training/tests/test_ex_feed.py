"""④ ex-feed 纠错对派生单测。"""

from datetime import UTC, datetime

import pytest
from lethefield_clients import ExEvent, FeedKind
from lethefield_training import ex_feed


def _event(n, event_id, content, ref_conflict=None):
    return ExEvent(
        n=n,
        event_id=event_id,
        content=content,
        agent_actor_id=None,
        account_id=None,
        tau_ms=None,
        ref_conflict=ref_conflict,
        created_at=datetime(2026, 8, 8, tzinfo=UTC),
    )


def test_collect_correction_pairs():
    events = [
        _event(1, "aaa", "旧内容"),
        _event(2, "bbb", "新内容", ref_conflict="ev_aaa"),
        _event(3, "ccc", "普通事件"),
    ]
    pairs = ex_feed.collect_correction_pairs(events)
    assert len(pairs) == 1
    pair = pairs[0]
    assert pair["old_node_key"] == "ev_aaa"
    assert pair["new_node_key"] == "ev_bbb"
    assert pair["before"] == "旧内容"
    assert pair["after"] == "新内容"


def test_collect_skips_missing_old_event():
    # 旧事件不在 EX（已归档/键不符过渡约定）→ 缺一半的对不喂
    events = [_event(2, "bbb", "新内容", ref_conflict="ev_gone")]
    assert ex_feed.collect_correction_pairs(events) == []


class _FakeSession:
    def __init__(self, events):
        self._events = events


class _FakeRegistry:
    def __init__(self, authorized):
        self.authorized = authorized

    def is_authorized(self, space_ref, scope):
        return self.authorized


def test_run_unauthorized_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(ex_feed, "list_experience_events", lambda session, *, space_id: [])
    with pytest.raises(PermissionError, match="入 topic 前拦截"):
        ex_feed.run(
            _FakeSession([]),
            space_id="space_x",
            registry=_FakeRegistry(authorized=False),
            publish=lambda e: None,
            state_path=tmp_path / "state.json",
        )


def test_run_publishes_new_pairs_idempotent(tmp_path, monkeypatch):
    events = [
        _event(1, "aaa", "旧内容"),
        _event(2, "bbb", "新内容", ref_conflict="ev_aaa"),
    ]
    monkeypatch.setattr(ex_feed, "list_experience_events", lambda session, *, space_id: events)
    published = []
    state = tmp_path / "state.json"
    count = ex_feed.run(
        _FakeSession(events),
        space_id="space_x",
        registry=_FakeRegistry(authorized=True),
        publish=published.append,
        state_path=state,
    )
    assert count == 1
    assert published[0].kind is FeedKind.CORRECTION_PAIR
    # 重跑幂等：已喂对子不重复
    assert (
        ex_feed.run(
            _FakeSession(events),
            space_id="space_x",
            registry=_FakeRegistry(authorized=True),
            publish=published.append,
            state_path=state,
        )
        == 0
    )
    assert len(published) == 1
