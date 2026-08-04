"""ops 最小实现的集成测试：决策留痕表单 + 授权注册表（M0 验收项）。

需要 compose 栈中的 PostgreSQL 已就绪。
"""

import uuid

import pytest
from lethefield_auth_registry import AuthRegistryStore, AuthScope, AuthStatus
from lethefield_decision_log import DecisionLogStore


def test_decision_log_submit_and_query():
    store = DecisionLogStore()
    marker = uuid.uuid4().hex[:8]
    record_id = store.submit(
        title=f"M0 集成测试决策 {marker}",
        context="CI 基线验证",
        decision="采用方案 A",
        rationale="与文档定案一致",
        decided_by="ci-bot",
    )
    record = store.get(record_id)
    assert record is not None
    assert record.title.endswith(marker)
    assert record.decision == "采用方案 A"
    assert record.decided_by == "ci-bot"

    recent = store.list(limit=5)
    assert any(r.id == record_id for r in recent)


def test_decision_log_get_missing_returns_none():
    assert DecisionLogStore().get(-1) is None


def test_auth_registry_grant_revoke_cycle():
    store = AuthRegistryStore()
    space_ref = f"test-{uuid.uuid4().hex}"

    # 空表/未知条目：查询不报错，授权判定为拒绝
    assert store.get(space_ref) is None
    assert not store.is_authorized(space_ref, AuthScope.CALIBRATION)

    store.grant(space_ref, [AuthScope.CALIBRATION])
    assert store.is_authorized(space_ref, AuthScope.CALIBRATION)
    # 粒度授权：未授予的 scope 不通过
    assert not store.is_authorized(space_ref, AuthScope.CONTENT_COPY)

    entry = store.get(space_ref)
    assert entry.status == AuthStatus.ACTIVE
    assert entry.scopes == (AuthScope.CALIBRATION,)

    assert store.revoke(space_ref) is True
    assert not store.is_authorized(space_ref, AuthScope.CALIBRATION)
    assert store.get(space_ref).status == AuthStatus.REVOKED

    # grant 幂等：撤回后重新授权恢复 active
    store.grant(space_ref, [AuthScope.CALIBRATION, AuthScope.CONTENT_COPY])
    assert store.is_authorized(space_ref, AuthScope.CONTENT_COPY)

    # 清理：撤回测试条目，保持注册表干净
    store.revoke(space_ref)


def test_auth_registry_revoke_missing_returns_false():
    store = AuthRegistryStore()
    assert store.revoke(f"test-{uuid.uuid4().hex}") is False


def test_auth_registry_list_on_empty_table():
    store = AuthRegistryStore()
    space_ref = f"test-{uuid.uuid4().hex}"
    assert all(e.space_ref != space_ref for e in store.list())


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
