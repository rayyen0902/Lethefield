"""corrections 单测（M7）：幂等决策、固化锁定透传、pending 分支。

图访问以 FakeClient 替身覆盖（脚本按内容分发预设结果）；原子性/真实幂等
由集成测试在真实图上断言。
"""

from datetime import UTC, datetime

import pytest
from lethefield_clients.ex_n import ExEvent
from lethefield_rms import corrections


class _Submitted:
    def __init__(self, result) -> None:
        self._result = result

    def all(self) -> "_Submitted":
        return self

    def result(self):
        return self._result


def _phi_entries(s=0.8, n_lt=5, n_star=16, rc=0, cc=0, nc=0, consolidated=False):
    entries = [
        {"s": [s]},
        {"n_last_touched": [n_lt]},
        {"n_star_cached": [n_star]},
        {"reinforce_count": [rc]},
        {"conflict_count": [cc]},
        {"neglect_count": [nc]},
    ]
    if consolidated:
        entries.append({"consolidated_at": [datetime(2026, 1, 1, tzinfo=UTC)]})
    return entries


class FakeClient:
    """按脚本内容分发：valueMap → φ；refEx → 节点反查（按调用序弹出）；其余 → 施加结果。"""

    def __init__(self, *, phi_entries=None, refex_keys=None, apply_result=("ok",)) -> None:
        self.phi_entries = phi_entries
        self.refex_queue = list(refex_keys or [])
        self.apply_result = list(apply_result)
        self.calls: list[tuple[str, dict]] = []

    def submit(self, script: str, bindings: dict) -> _Submitted:
        self.calls.append((script, bindings))
        if "valueMap" in script:
            return _Submitted(self.phi_entries if self.phi_entries is not None else [])
        if "refEx" in bindings:
            keys = self.refex_queue.pop(0) if self.refex_queue else []
            return _Submitted([{"rows": keys}])
        return _Submitted(self.apply_result)


def test_apply_correction_ok_writes_delta_and_edge():
    client = FakeClient(phi_entries=_phi_entries(s=0.8, n_lt=5))
    outcome = corrections.apply_correction(
        client, "g", space_id="sp", new_node_key="new", old_node_key="old", n_now=10
    )
    assert outcome == "ok"
    _, bindings = client.calls[-1]
    assert bindings["newKey"] == "new" and bindings["oldKey"] == "old"
    assert bindings["sNew"] == pytest.approx(0.3)  # 0.8 − 0.5
    assert bindings["nNow"] == "10"  # conflict δ 更新 n_last_touched
    assert bindings["locked"] is False
    assert isinstance(bindings["nStar"], str)  # long 字符串绑定（int32 限制）


def test_apply_correction_duplicate_zero_write():
    client = FakeClient(phi_entries=_phi_entries(), apply_result=("duplicate",))
    outcome = corrections.apply_correction(
        client, "g", space_id="sp", new_node_key="new", old_node_key="old", n_now=10
    )
    assert outcome == "duplicate"  # 同对已存在边：脚本零写入，调用方只解释结果


def test_apply_correction_consolidated_locks_s():
    client = FakeClient(phi_entries=_phi_entries(s=0.8, consolidated=True))
    corrections.apply_correction(
        client, "g", space_id="sp", new_node_key="new", old_node_key="old", n_now=10
    )
    _, bindings = client.calls[-1]
    assert bindings["locked"] is True
    assert bindings["sNew"] == pytest.approx(0.8)  # 固化：s 锁定，仅计数器 +1


def _event(n: int, event_id: str, ref_conflict: str | None) -> ExEvent:
    return ExEvent(
        n=n,
        event_id=event_id,
        content=f"c{n}",
        agent_actor_id="a",
        account_id="acc",
        tau_ms=n * 1000,
        ref_conflict=ref_conflict,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_process_corrections_pending_when_new_node_not_ingested(monkeypatch):
    events = [_event(1, "e1", None), _event(2, "e2", "old")]
    monkeypatch.setattr(corrections, "list_experience_events", lambda s, *, space_id: events)
    client = FakeClient(refex_keys=[[]])  # ref_ex 反查无结果：新节点未入图
    stats = corrections.process_corrections(client, None, gname="g", space_id="sp", n_now=10)
    assert stats.pending == 1 and stats.applied == 0 and stats.duplicate == 0


def test_process_corrections_pending_when_old_node_missing(monkeypatch):
    events = [_event(2, "e2", "ghost")]
    monkeypatch.setattr(corrections, "list_experience_events", lambda s, *, space_id: events)
    # 新节点查得到，旧节点 read_phi 空结果 → KeyError → pending
    client = FakeClient(phi_entries=None, refex_keys=[["new"]])
    stats = corrections.process_corrections(client, None, gname="g", space_id="sp", n_now=10)
    assert stats.pending == 1 and stats.applied == 0


def test_process_corrections_applied_and_duplicate(monkeypatch):
    events = [_event(2, "e2", "old"), _event(3, "e3", "old")]
    monkeypatch.setattr(corrections, "list_experience_events", lambda s, *, space_id: events)
    client = FakeClient(phi_entries=_phi_entries(), refex_keys=[["new2"], ["new3"]])
    stats = corrections.process_corrections(client, None, gname="g", space_id="sp", n_now=10)
    assert stats.applied == 2  # 不同新节点 → 两条边两次 δ（幂等约束的是"同一对"）
    client2 = FakeClient(
        phi_entries=_phi_entries(), refex_keys=[["new2"], ["new3"]], apply_result=("duplicate",)
    )
    stats2 = corrections.process_corrections(client2, None, gname="g", space_id="sp", n_now=10)
    assert stats2.duplicate == 2 and stats2.applied == 0
