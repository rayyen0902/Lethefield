"""空间创建入口（M16，开发文档 §17 + 修订记录第 24 条⑥）。

顺序定案：校验账号（存在且 active）→ `validate_space_id` → 调 M9/M10 开通
流水线（三存储生命周期）→ **provision 成功后才写归属行**。任何一步失败都不
产生半开通状态：provision 内部已按逆序回滚存储，归属行不落库。
"""

from lethefield_clients import SpaceMapping, Tier, validate_space_id

from lethefield_is.store import AccountStatus, IsStore


class SpaceCreateError(RuntimeError):
    """空间创建入口前置校验失败（账号不存在/已停用；space_id 非法由 validate 抛）。"""


def create_space(
    is_store: IsStore,
    provision,  # Callable[[str, Tier], SpaceMapping]——注入 M9/M10 provision 绑定
    *,
    account_id: str,
    space_id: str,
    tier: Tier = Tier.COLD,
) -> SpaceMapping:
    """开通 space 并登记账号归属。provision 抛错上抛、归属行不落（无半开通状态）。"""
    account = is_store.get_account(account_id)
    if account is None:
        raise SpaceCreateError(f"账号不存在：{account_id!r}")
    if account.status is not AccountStatus.ACTIVE:
        raise SpaceCreateError(f"账号已停用：{account_id!r}")
    validate_space_id(space_id)  # fail-closed，非法 space_id 零副作用

    mapping = provision(space_id, tier)
    is_store.bind_space(account_id, space_id)
    return mapping
