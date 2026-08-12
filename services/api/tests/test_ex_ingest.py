"""ex_ingest 单测：reinforce 时间窗合并矩阵（M7）+ EX→Pulsar 生产侧（M14）。

合并规则：窗口内同节点同类元事件 → 同主键 UPDATE（count 累加、n_at_event 刷新，
不产生新行）；窗口外/无候选 → INSERT 新行。未传 merge_window_ms 恒 INSERT。

M14 生产侧定案：落库确认后发布 ex-events 信封；发布失败不阻塞同步返回
（EX 是 SoT），page 告警 + 指标。
"""

import uuid
from datetime import UTC, datetime, timedelta

from lethefield_api.ex_ingest import REINFORCE_MERGE_WINDOW_MS, append_experience, append_meta
from lethefield_api.stream_publisher import PublishError

KW = {
    "space_id": "demo",
    "node_key": "node-1",
    "meta_type": "reinforce",
    "n_at_event": 7,
    "agent_actor_id": "actor",
    "account_id": "acc",
}


class MergeRow:
    def __init__(self, count: int) -> None:
        self.node_key = "node-1"
        self.created_at = datetime.now(UTC) - timedelta(seconds=5)
        self.event_id = uuid.uuid4()
        self.meta_type = "reinforce"
        self.count = count


class FakeSession:
    """预设合并查询结果（one()），记录全部 CQL 与参数。"""

    def __init__(self, merge_row=None) -> None:
        self.merge_row = merge_row
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, query: str, params: tuple = ()):
        self.calls.append((query, params))
        return self

    def one(self):
        return self.merge_row


def _kinds(session: FakeSession) -> list[str]:
    return [q.strip().split()[0].upper() for q, _ in session.calls]


def test_merge_within_window_updates_count():
    session = FakeSession(merge_row=MergeRow(count=2))
    returned = append_meta(session, **KW, merge_window_ms=REINFORCE_MERGE_WINDOW_MS)
    assert _kinds(session) == ["SELECT", "UPDATE"]  # 无 INSERT——一行合并
    _, params = session.calls[1]
    assert params[0] == 3  # count 2+1 累加
    assert params[1] == 7  # n_at_event 刷新为最新
    assert returned == str(session.merge_row.event_id)  # 返回被合并行 id


def test_merge_without_candidate_inserts_new_row():
    session = FakeSession(merge_row=None)  # 窗口内无候选
    append_meta(session, **KW, merge_window_ms=REINFORCE_MERGE_WINDOW_MS)
    assert _kinds(session) == ["SELECT", "INSERT"]
    _, params = session.calls[1]
    assert params[4] == 1  # count=1


def test_no_window_always_inserts():
    session = FakeSession(merge_row=MergeRow(count=5))  # 即使有候选也不查不合
    append_meta(session, **KW)  # 未传 merge_window_ms
    assert _kinds(session) == ["INSERT"]


def test_merge_window_boundary_passed_to_query():
    session = FakeSession(merge_row=None)
    append_meta(session, **KW, merge_window_ms=60_000)
    query, params = session.calls[0]
    assert "created_at >= %s" in query
    since = params[1]
    assert datetime.now(UTC) - since < timedelta(seconds=61)  # 窗口下界 ≈ now-60s


# ------------------------------------------- EX→Pulsar 生产侧（M14）


class FakeRedis:
    def __init__(self) -> None:
        self._data: dict[str, int] = {}

    def incr(self, key: str) -> int:
        self._data[key] = self._data.get(key, 0) + 1
        return self._data[key]

    def set(self, key: str, value) -> None:
        self._data[key] = value


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list = []

    def publish(self, event) -> None:
        if self.fail:
            raise PublishError("broker 不可达（fake）")
        self.published.append(event)


def test_append_experience_publishes_after_commit():
    """落库确认后发布 ex-events 信封：字段与 EX 行一致。"""
    session = FakeSession()
    redis = FakeRedis()
    publisher = FakePublisher()
    event_id, n = append_experience(
        session,
        redis,
        space_id="demo",
        content="内容",
        agent_actor_id="actor",
        account_id="acc",
        tau_ms=123,
        publisher=publisher,
    )
    assert n == 1
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.space_id == "demo"
    assert event.event_id == event_id and event.n == 1
    assert event.content == "内容" and event.tau_ms == 123
    assert event.agent_actor_id == "actor" and event.created_at_ms > 0


def test_append_experience_without_publisher_skips():
    """publisher=None（测试/工具路径）：不发布，行为同 M14 前。"""
    session = FakeSession()
    event_id, n = append_experience(
        session,
        FakeRedis(),
        space_id="demo",
        content="c",
        agent_actor_id="a",
        account_id="acc",
    )
    assert event_id and n == 1


def test_append_experience_publish_failure_does_not_block():
    """发布失败不阻塞同步返回（EX 是 SoT）：正常返回 (event_id, n)，不抛异常。"""
    session = FakeSession()
    publisher = FakePublisher(fail=True)
    event_id, n = append_experience(
        session,
        FakeRedis(),
        space_id="demo",
        content="c",
        agent_actor_id="a",
        account_id="acc",
        publisher=publisher,
    )
    assert event_id and n == 1  # 同步返回语义不变
    assert publisher.published == []
