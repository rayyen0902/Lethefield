"""开通流水线（M9/M10 统一顺序）：EX → Pulsar → RMS+ES → 注册映射，失败回滚。

设计文档 §17.2/§18.1：
- 选水位最低的 open Cell（无 open → 零副作用终止）；
- **先存储后注册**——存储步骤失败时注册不执行，且已完成的存储资源被回滚清理
  （M9 验收硬指标）；注册本身失败同样回滚存储；
- hot/premium tier 注册后预 open 图实例，消化冷开 ~3.6s 延迟（§17.2）。

回滚是尽力而为：某步回滚失败抛 ProvisionRollbackError 携带现场（不静默），
由运维按残留清单处理（开通失败的 space 未注册映射，不影响存量读写）。
"""

from collections.abc import Callable
from dataclasses import dataclass

from cassandra.cluster import Session
from gremlin_python.driver.client import Client
from lethefield_clients import (
    MappingTableControlPlaneStore,
    SpaceMapping,
    SpaceNotFoundError,
    Tier,
    ensure_ex_keyspace,
    keyspace_name,
    validate_space_id,
)
from lethefield_rms.schema import backend_props_of, ensure_graph_schema

from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import DDL_TIMEOUT_SECONDS as _DDL_TIMEOUT_SECONDS
from lethefield_scheduler.config import DEFAULT_CONFIG, SchedulerConfig
from lethefield_scheduler.watermark import select_cell


class ProvisionError(RuntimeError):
    """开通失败（存储已回滚清理）。"""


class ProvisionRollbackError(ProvisionError):
    """开通失败且回滚未清干净——携带原始异常与回滚异常清单，需人工处理残留。"""


@dataclass
class ProvisionDeps:
    """开通流程依赖容器（构造时注入，测试逐项替换为 fake）。"""

    store: MappingTableControlPlaneStore
    gremlin: Client
    ex_session: Session
    cell_session: Session
    config: SchedulerConfig = DEFAULT_CONFIG


def _rollback_graph(deps: ProvisionDeps, space_id: str) -> None:
    """回滚 RMS 步：驱逐图实例 + 移除图配置 + 清掉可能已建的图 keyspace。

    顺序对齐红线 5：close/removeConfiguration 先于 DROP KEYSPACE。
    close 对未 open 的图容错（幂等），removeConfiguration 严格——驱逐不完整
    直接失败，绝不带开着的图实例进 DROP。
    """
    deps.gremlin.submit(
        "if (ConfiguredGraphFactory.getGraphNames().contains(gname)) { "
        "try { ConfiguredGraphFactory.close(gname) } catch (Exception ignored) {}; "
        "ConfiguredGraphFactory.removeConfiguration(gname) "
        "}; 'ok'",
        {"gname": space_id},
    ).all().result()
    deps.cell_session.execute(f"DROP KEYSPACE IF EXISTS {space_id}", timeout=_DDL_TIMEOUT_SECONDS)


def provision_space(
    deps: ProvisionDeps,
    space_id: str,
    *,
    tier: Tier = Tier.COLD,
) -> SpaceMapping:
    """开通一个 space：选 Cell → EX → Pulsar → RMS 建图+schema → 注册映射。

    幂等：已注册映射直接返回（先存储后注册保证已注册即存储齐备）。
    """
    validate_space_id(space_id)  # fail-closed，不合法 space_id 零副作用
    try:
        return deps.store.get_space_mapping(space_id)
    except SpaceNotFoundError:
        pass

    cell = select_cell(deps.store)  # 无 open Cell：零副作用终止

    rollbacks: list[tuple[str, Callable[[], None]]] = []
    try:
        # 1. EX keyspace 先行（source of truth 先于派生物，§18.1）
        ensure_ex_keyspace(deps.ex_session, space_id)
        rollbacks.append(
            (
                "drop_ex_keyspace",
                lambda: deps.ex_session.execute(
                    f"DROP KEYSPACE IF EXISTS {keyspace_name(space_id)}",
                    timeout=_DDL_TIMEOUT_SECONDS,
                ),
            )
        )
        # 2. Pulsar namespace（全局集群池）+ namespace 级配额/策略（v0.7 选型核心理由落地）
        pulsar_admin.ensure_namespace(
            deps.config.pulsar_admin_url, deps.config.pulsar_tenant, space_id
        )
        pulsar_admin.set_retention(
            deps.config.pulsar_admin_url,
            deps.config.pulsar_tenant,
            space_id,
            minutes=deps.config.pulsar_namespace_retention_minutes,
            size_mb=deps.config.pulsar_namespace_retention_size_mb,
        )
        pulsar_admin.set_backlog_quota(
            deps.config.pulsar_admin_url,
            deps.config.pulsar_tenant,
            space_id,
            quota_mb=deps.config.pulsar_namespace_backlog_quota_mb,
        )
        rollbacks.append(
            (
                "delete_pulsar_namespace",
                lambda: pulsar_admin.delete_namespace(
                    deps.config.pulsar_admin_url, deps.config.pulsar_tenant, space_id
                ),
            )
        )
        # 3. RMS 建图 + schema（后端端点按 Cell 映射推导）+ ES 侧随图就绪
        ensure_graph_schema(deps.gremlin, space_id, backend_props=backend_props_of(cell.endpoints))
        rollbacks.append(("rollback_graph", lambda: _rollback_graph(deps, space_id)))
        # 4. 注册映射（先存储后注册）
        mapping = SpaceMapping(
            space_id=space_id,
            cell_id=cell.cell_id,
            ex_cluster_id=deps.config.ex_cluster_id,
            pulsar_cluster_id=deps.config.pulsar_cluster_id,
            tier=tier,
        )
        deps.store.register_space(mapping)
    except Exception as exc:
        rollback_errors: list[tuple[str, Exception]] = []
        for name, rollback in reversed(rollbacks):
            try:
                rollback()
            except Exception as rb_exc:  # noqa: BLE001 — 回滚尽力而为，异常收进清单
                rollback_errors.append((name, rb_exc))
        if rollback_errors:
            raise ProvisionRollbackError(
                f"开通 {space_id} 失败（{exc}），且回滚残留："
                + ", ".join(f"{name}: {e}" for name, e in rollback_errors)
            ) from exc
        raise ProvisionError(f"开通 {space_id} 失败，存储已回滚：{exc}") from exc

    # hot/premium tier 预 open 图实例（§17.2：消化冷开 ~3.6s 延迟）
    if tier in (Tier.HOT, Tier.PREMIUM):
        deps.gremlin.submit(
            "ConfiguredGraphFactory.open(gname); 'preopened'", {"gname": space_id}
        ).all().result()
    return mapping
