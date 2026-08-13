"""IS 空间创建入口单测（M16）：顺序与失败路径（无半开通状态）。

无栈：IsStore 与 provision 均用 fake——编排逻辑全 fake 验证，真实组件行为归
集成测试 test_m16_is.py。
"""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from lethefield_clients import Tier
from lethefield_is.service import SpaceCreateError, create_space
from lethefield_is.store import Account, AccountStatus


class FakeIsStore:
    def __init__(self, account: Account | None) -> None:
        self._account = account
        self.bound: list[tuple[str, str]] = []

    def get_account(self, account_id: str):
        return self._account

    def bind_space(self, account_id: str, space_id: str) -> None:
        self.bound.append((account_id, space_id))


def _account(status: AccountStatus = AccountStatus.ACTIVE) -> Account:
    now = datetime.now(UTC)
    return Account(
        account_id="acct_1",
        display_name="",
        status=status,
        created_at=now,
        updated_at=now,
    )


def _mapping(space_id: str):
    return SimpleNamespace(space_id=space_id, cell_id="cell-local", tier=Tier.COLD)


def test_create_space_success_binds_after_provision():
    calls: list[str] = []

    def provision(space_id, tier):
        calls.append("provision")
        return _mapping(space_id)

    store = FakeIsStore(_account())
    mapping = create_space(store, provision, account_id="acct_1", space_id="space_a")

    assert mapping.space_id == "space_a"
    assert store.bound == [("acct_1", "space_a")]
    assert calls == ["provision"]  # provision 成功后才写归属行


def test_provision_failure_leaves_no_ownership():
    def provision(space_id, tier):
        raise RuntimeError("pulsar down")

    store = FakeIsStore(_account())
    with pytest.raises(RuntimeError, match="pulsar down"):
        create_space(store, provision, account_id="acct_1", space_id="space_a")
    assert store.bound == []  # 无半开通状态：归属行不落


def test_missing_account_rejected_before_provision():
    def provision(space_id, tier):
        raise AssertionError("不应被调用")

    store = FakeIsStore(None)
    with pytest.raises(SpaceCreateError, match="账号不存在"):
        create_space(store, provision, account_id="ghost", space_id="space_a")


def test_disabled_account_rejected_before_provision():
    def provision(space_id, tier):
        raise AssertionError("不应被调用")

    store = FakeIsStore(_account(AccountStatus.DISABLED))
    with pytest.raises(SpaceCreateError, match="账号已停用"):
        create_space(store, provision, account_id="acct_1", space_id="space_a")


def test_invalid_space_id_rejected_before_provision():
    def provision(space_id, tier):
        raise AssertionError("不应被调用")

    store = FakeIsStore(_account())
    with pytest.raises(ValueError):
        create_space(store, provision, account_id="acct_1", space_id="INVALID-UPPER")
    assert store.bound == []
