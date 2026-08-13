"""M17 命令业务逻辑（deps 注入，可单测）。

每条命令绑定显式 space/cell 参数（红线 1 在操作面的落实），不存在
"对全部 space 执行"的全局形态；留痕包装在 audit.py（__main__ 统一接线）。

跨包复用 scheduler/rms/training 的业务函数作库调用（先例：IS 复用
provision_space，services/is/pyproject.toml）。
"""

from lethefield_clients import (
    AuthRegistryStore,
    CellInfo,
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    pulsar_client,
    space_ref_of,
)
from lethefield_rms.quota import QuotaCounters
from lethefield_scheduler.config import DEFAULT_CONFIG, cell_host_endpoints
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.migrate import MigrateDeps, MigrationReport, migrate_space
from lethefield_scheduler.watermark import refresh_cell, select_cell
from lethefield_training.hot_store import HotSampleStore

from lethefield_ops_cli.audit import CommandResult


def cmd_space_status(
    store: MappingTableControlPlaneStore, counters: QuotaCounters, space_ids: list[str]
) -> CommandResult:
    """space 状态查询：映射（cell/status/tier）+ 所在 Cell 水位 + 配额用量。

    配额用量为近似值（图计数带 TTL 缓存，红线 2 近似执行语义），输出注明。
    """
    lines: list[str] = []
    for space_id in space_ids:
        mapping = store.get_space_mapping(space_id)  # 不存在 → SpaceNotFoundError 走失败路径
        cell = store.get_cell(mapping.cell_id)
        # 图名 = space_id（M5 定案）
        vertices = counters.vertex_count(space_id)
        edges = counters.edge_count(space_id)
        vectors = counters.vector_count(space_id)
        lines.append(
            f"space {space_id} cell={mapping.cell_id} status={mapping.status} tier={mapping.tier}"
        )
        lines.append(f"  cell 水位 state={cell.watermark_state} capacity={cell.capacity}")
        lines.append(
            f"  配额用量（近似，图计数带 TTL 缓存）："
            f"vertices={vertices} edges={edges} vectors={vectors}"
        )
    return CommandResult(0, f"状态查询：{', '.join(space_ids)}", tuple(lines))


def cmd_set_tier(store: MappingTableControlPlaneStore, space_id: str, tier: Tier) -> CommandResult:
    """tier 升降调整（只改映射 tier 字段；配额档位/sweep 分频随即按新 tier 生效）。"""
    before = store.get_space_mapping(space_id)  # 不存在则 fail-closed
    store.update_space_tier(space_id, tier)
    return CommandResult(
        0, f"space {space_id} tier {before.tier} → {tier}", (f"[ok] {space_id} tier={tier}",)
    )


def cmd_destroy(store: MappingTableControlPlaneStore, cell_cluster, space_id: str) -> CommandResult:
    """整 space 销毁处置：触发 M9/M10 注销流水线（含契约 5 广播，等 broker ack）。

    失败中止时映射留 destroying，步骤幂等——重跑本命令即重试（M10 定案语义）。
    """
    gremlin = gremlin_client()
    ex_cluster = ex_cassandra_cluster()
    pulsar = pulsar_client()
    try:
        deps = DestroyDeps(
            store=store,
            gremlin=gremlin,
            cell_session=cell_cluster.connect(),
            ex_session=ex_cluster.connect(),
            es=es_client(),
            config=DEFAULT_CONFIG,
            pulsar=pulsar,
        )
        destroy_space(deps, space_id)
        return CommandResult(0, f"space {space_id} 已注销，无残留")
    finally:
        pulsar.close()
        gremlin.close()
        ex_cluster.shutdown()


def _migrate_one(
    store: MappingTableControlPlaneStore,
    space_id: str,
    to_cell_id: str | None,
    grace_seconds: float,
) -> MigrationReport:
    """单 space 迁移（deps 组装与 scheduler CLI 同路径；gremlin 单连接不跨线程）。"""
    mapping = store.get_space_mapping(space_id)
    source_ep = cell_host_endpoints(mapping.cell_id)
    target_id = to_cell_id
    if target_id is None:
        target_id = select_cell(store, exclude=frozenset({mapping.cell_id})).cell_id
    target_ep = cell_host_endpoints(target_id)
    source_cluster = (
        cassandra_cluster(port=int(source_ep["cassandra_port"]))
        if source_ep["cassandra_port"]
        else cassandra_cluster()
    )
    target_cluster = (
        cassandra_cluster(port=int(target_ep["cassandra_port"]))
        if target_ep["cassandra_port"]
        else source_cluster
    )
    source_gremlin = gremlin_client(source_ep["gremlin_url"])
    target_gremlin = gremlin_client(target_ep["gremlin_url"])
    ex_cluster = ex_cassandra_cluster()
    try:
        deps = MigrateDeps(
            store=store,
            source_gremlin=source_gremlin,
            target_gremlin=target_gremlin,
            source_cell_session=source_cluster.connect(),
            target_cell_session=target_cluster.connect(),
            source_es=es_client(source_ep["es_url"]),
            target_es=es_client(target_ep["es_url"]),
            ex_session=ex_cluster.connect(),
            config=DEFAULT_CONFIG,
        )
        return migrate_space(deps, space_id, to_cell_id=to_cell_id, grace_seconds=grace_seconds)
    finally:
        source_gremlin.close()
        target_gremlin.close()
        ex_cluster.shutdown()
        if target_cluster is not source_cluster:
            target_cluster.shutdown()
        source_cluster.shutdown()


def _report_result(report: MigrationReport) -> CommandResult:
    detail = (
        f"space {report.space_id} 已迁移 {report.source_cell_id} → {report.target_cell_id}，"
        f"只读窗口 {report.read_only_window_seconds}s"
    )
    return CommandResult(0, detail, (f"[ok] {detail}（步骤耗时 {report.step_seconds}）",))


def cmd_migrate_rebalance(
    store: MappingTableControlPlaneStore, space_id: str, grace_seconds: float = 0.0
) -> CommandResult:
    """迁移触发·再平衡：自动选水位最低的 open Cell（排除当前 Cell）。"""
    return _report_result(_migrate_one(store, space_id, None, grace_seconds))


def cmd_migrate_to_cell(
    store: MappingTableControlPlaneStore, space_id: str, to_cell_id: str, grace_seconds: float = 0.0
) -> CommandResult:
    """迁移触发·跨集群/指定目标：显式目标 Cell。"""
    return _report_result(_migrate_one(store, space_id, to_cell_id, grace_seconds))


def cmd_migrate_evacuate(
    store: MappingTableControlPlaneStore,
    cell_id: str,
    space_ids: list[str],
    grace_seconds: float = 0.0,
) -> CommandResult:
    """迁移触发·Cell 退役：显式 space 列表逐一迁出该 Cell（目标自动选、排除本 Cell）。

    红线 1：space 列表必须显式给出，不提供"迁出该 Cell 全部 space"的全局形态。
    """
    for space_id in space_ids:
        mapping = store.get_space_mapping(space_id)
        if mapping.cell_id != cell_id:
            raise ValueError(
                f"space {space_id!r} 归属 {mapping.cell_id}，不在待退役 Cell {cell_id} 上"
            )
    lines: list[str] = []
    for space_id in space_ids:
        lines.extend(_report_result(_migrate_one(store, space_id, None, grace_seconds)).lines)
    return CommandResult(0, f"Cell {cell_id} 退役迁出：{', '.join(space_ids)}", tuple(lines))


def cmd_auth_revoke(
    registry: AuthRegistryStore, hot_store: HotSampleStore, space_id: str
) -> CommandResult:
    """授权撤回处置（M11 流程）：注册表撤回（停新增）+ 热层 scrub（清存量，幂等）。"""
    space_ref = space_ref_of(space_id)
    existed = registry.revoke(space_ref)
    scrubbed = hot_store.scrub(space_ref)
    registry_result = "已撤回" if existed else "无条目"
    detail = f"space {space_id} 授权撤回：注册表 {registry_result}，存量处置 {scrubbed} 条"
    # 注册表无该 space 条目 → 按 IS CLI 同口径报 not found（可能 space 拼错）；scrub 已幂等执行
    return CommandResult(0 if existed else 1, detail)


def cmd_cell_watermark(
    store: MappingTableControlPlaneStore,
    cell_id: str,
    *,
    refresh: bool = False,
    cell_cluster=None,
) -> CommandResult:
    """Cell 水位查看；--refresh 时现场探测并刷新映射表水位状态。"""
    if refresh:
        cell = refresh_cell(store, cell_id, cell_session=cell_cluster.connect(), es=es_client())
    else:
        cell = store.get_cell(cell_id)  # 不存在 → KeyError 走失败路径
    line = (
        f"cell {cell.cell_id} state={cell.watermark_state} "
        f"capacity={cell.capacity} endpoints={cell.endpoints}"
    )
    return CommandResult(0, f"Cell {cell_id} 水位 {cell.watermark_state}", (line,))


# 建图配置推导必需键（lethefield_rms.schema.backend_props_of 按这两个键取端点）
_REQUIRED_ENDPOINT_KEYS = ("cassandra", "es")


def cmd_cell_register(
    store: MappingTableControlPlaneStore, cell_id: str, endpoints: dict[str, str]
) -> CommandResult:
    """新 Cell 筹备触发：映射表登记 Cell（INSERT 覆盖写幂等）。

    Cell 基础设施（容器/机器）由运维自备，本命令只收口"筹备完成、可参与调度"
    的触发点；登记后 select_cell 可将新 space 路由到该 Cell。
    """
    missing = [k for k in _REQUIRED_ENDPOINT_KEYS if k not in endpoints]
    if missing:
        raise ValueError(f"endpoints 缺少必需键 {missing}（建图配置推导需要，backend_props_of）")
    store.register_cell(CellInfo(cell_id=cell_id, endpoints=dict(endpoints)))
    return CommandResult(
        0, f"Cell {cell_id} 已登记（endpoints={endpoints}）", (f"[ok] cell {cell_id} 已登记",)
    )
