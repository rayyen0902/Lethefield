"""ex_ingest 单测：reinforce 时间窗合并矩阵（M7）。

合并规则：窗口内同节点同类元事件 → 同主键 UPDATE（count 累加、n_at_event 刷新，
不产生新行）；窗口外/无候选 → INSERT 新行。未传 merge_window_ms 恒 INSERT。
"""

import uuid
from datetime import UTC, datetime, timedelta

from lethefield_api.ex_ingest import REINFORCE_MERGE_WINDOW_MS, append_meta

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
