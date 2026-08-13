"""M17 留痕包装（audit.run_with_audit）单测：预检/执行/留痕三阶段语义。"""

import pytest
from lethefield_ops_cli import audit
from lethefield_ops_cli.audit import CommandResult


class FakeDecisionStore:
    def __init__(self, fail: bool = False) -> None:
        self.submissions: list[dict] = []
        self.fail = fail

    def submit(self, **kwargs):
        if self.fail:
            raise RuntimeError("pg down")
        self.submissions.append(kwargs)
        return 42


@pytest.fixture(autouse=True)
def _precheck_ok(monkeypatch):
    monkeypatch.setattr(audit, "_precheck", lambda dsn: None)


def test_success_logs_and_returns_zero(capsys):
    store = FakeDecisionStore()
    code = audit.run_with_audit(
        operator="ops-a",
        title="ops: space status --space s1",
        decision="查询 space 状态：s1",
        fn=lambda: CommandResult(0, "状态查询：s1", ("space s1 ...",)),
        store=store,
    )
    assert code == 0
    assert len(store.submissions) == 1
    sub = store.submissions[0]
    assert sub["decided_by"] == "ops-a"
    assert sub["title"] == "ops: space status --space s1"
    assert sub["outcome"] == "accepted"  # 枚举语义不拉伸，成败记 context.result
    assert '"result": "ok"' in sub["context"]
    assert "留痕 #42" in capsys.readouterr().out


def test_business_failure_still_logged():
    store = FakeDecisionStore()

    def boom():
        raise ValueError("space 不存在")

    code = audit.run_with_audit(operator="ops-a", title="t", decision="d", fn=boom, store=store)
    assert code == 1
    sub = store.submissions[0]
    assert '"result": "error"' in sub["context"]
    assert "ValueError" in sub["context"]


def test_precheck_failure_refuses_execution(monkeypatch):
    monkeypatch.setattr(
        audit, "_precheck", lambda dsn: (_ for _ in ()).throw(RuntimeError("pg unreachable"))
    )
    called = []
    code = audit.run_with_audit(
        operator="ops-a",
        title="t",
        decision="d",
        fn=lambda: called.append(1) or CommandResult(0, "x"),
        store=FakeDecisionStore(),
    )
    assert code == 1
    assert called == []  # 留痕库不可达 → 业务未执行（fail-closed）


def test_audit_write_failure_after_execution_returns_2(capsys):
    code = audit.run_with_audit(
        operator="ops-a",
        title="t",
        decision="d",
        fn=lambda: CommandResult(0, "已注销"),
        store=FakeDecisionStore(fail=True),
    )
    assert code == audit.EXIT_AUDIT_WRITE_FAILED
    assert "人工补录" in capsys.readouterr().err


def test_resolve_operator_priority(monkeypatch):
    monkeypatch.setenv("LETHEFIELD_OPERATOR", "env-ops")
    assert audit.resolve_operator("cli-ops") == "cli-ops"
    assert audit.resolve_operator(None) == "env-ops"
    monkeypatch.delenv("LETHEFIELD_OPERATOR")
    monkeypatch.setattr(audit.getpass, "getuser", lambda: "os-user")
    assert audit.resolve_operator(None) == "os-user"
