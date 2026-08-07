"""租户调度器 CLI（M9/M10）。

用法：
    python -m lethefield_scheduler bootstrap                # 控制面表 + 注册 Cell + 训练 namespace
    python -m lethefield_scheduler provision <space> [--tier cold|hot|premium]
    python -m lethefield_scheduler destroy <space>
    python -m lethefield_scheduler migrate <space> [--to-cell ID] [--grace-seconds N]
    python -m lethefield_scheduler watermark [--cell ID]    # 探测并刷新水位状态
    python -m lethefield_scheduler export <file>            # 映射表备份（1.0 验收硬指标）
    python -m lethefield_scheduler restore <file>           # 从备份恢复（幂等）
    python -m lethefield_scheduler list                     # 列出 Cell 与 space 映射
    python -m lethefield_scheduler.training_control_sink    # 契约 5 销毁指令最小接收 consumer
"""

import argparse

from lethefield_clients import (
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    export_jsonl,
    gremlin_client,
    local_cell,
    pulsar_client,
    restore_jsonl,
)

from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import DEFAULT_CONFIG, cell_host_endpoints
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.migrate import MigrateDeps, migrate_space
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_scheduler.watermark import refresh_cell, select_cell


def _store(cluster) -> MappingTableControlPlaneStore:
    store = MappingTableControlPlaneStore(cluster.connect())
    store.ensure_tables()
    return store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="租户调度器（M9）")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("bootstrap", help="建控制面表 + 注册本地 Cell（幂等）")
    p_provision = sub.add_parser("provision", help="开通 space（EX → Pulsar → RMS → 注册）")
    p_provision.add_argument("space_id")
    p_provision.add_argument("--tier", choices=[t.value for t in Tier], default=Tier.COLD.value)
    p_destroy = sub.add_parser("destroy", help="注销 space（先驱逐计算实例，再删存储）")
    p_destroy.add_argument("space_id")
    p_watermark = sub.add_parser("watermark", help="探测并刷新 Cell 水位状态")
    p_watermark.add_argument("--cell", default=None, help="只刷新指定 Cell（缺省全部）")
    p_migrate = sub.add_parser("migrate", help="跨 Cell 迁移 space（M10，实测只读窗口）")
    p_migrate.add_argument("space_id")
    p_migrate.add_argument("--to-cell", default=None, help="目标 Cell（缺省自动选水位最低）")
    p_migrate.add_argument("--grace-seconds", type=float, default=0.0, help="源侧清理前宽限期")
    p_export = sub.add_parser("export", help="映射表全量导出 JSONL")
    p_export.add_argument("file")
    p_restore = sub.add_parser("restore", help="从 JSONL 恢复映射表（幂等）")
    p_restore.add_argument("file")
    sub.add_parser("list", help="列出 Cell 与 space 映射")
    args = parser.parse_args(argv)

    cell_cluster = cassandra_cluster()
    store = _store(cell_cluster)
    try:
        if args.command == "bootstrap":
            try:
                store.get_cell(local_cell().cell_id)
            except KeyError:
                store.register_cell(local_cell())
            # 训练管线控制面就绪（契约 5）：独立 tenant/namespace + 审计级 retention
            pulsar_admin.ensure_namespace(
                DEFAULT_CONFIG.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE
            )
            pulsar_admin.set_retention(
                DEFAULT_CONFIG.pulsar_admin_url,
                TRAINING_TENANT,
                CONTROL_NAMESPACE,
                minutes=DEFAULT_CONFIG.training_control_retention_minutes,
                size_mb=-1,
            )
            print(f"[ok] 控制面就绪，Cell {local_cell().cell_id} 已注册，训练控制 namespace 已备妥")
            return 0
        if args.command == "export":
            count = export_jsonl(store, args.file)
            print(f"[ok] 已导出 {count} 行到 {args.file}")
            return 0
        if args.command == "restore":
            count = restore_jsonl(store, args.file)
            print(f"[ok] 已恢复 {count} 行（覆盖写，幂等）")
            return 0
        if args.command == "list":
            for cell in store.list_cells():
                print(
                    f"cell {cell.cell_id} state={cell.watermark_state} "
                    f"capacity={cell.capacity} endpoints={cell.endpoints}"
                )
            for mapping in store.list_space_mappings():
                print(
                    f"space {mapping.space_id} cell={mapping.cell_id} "
                    f"status={mapping.status} tier={mapping.tier}"
                )
            return 0

        if args.command == "watermark":
            cell_ids = [args.cell] if args.cell else [c.cell_id for c in store.list_cells()]
            cell_session = cell_cluster.connect()
            es = es_client()
            for cell_id in cell_ids:
                cell = refresh_cell(store, cell_id, cell_session=cell_session, es=es)
                print(f"[ok] {cell.cell_id} state={cell.watermark_state} capacity={cell.capacity}")
            return 0

        gremlin = gremlin_client()
        ex_cluster = ex_cassandra_cluster()
        try:
            if args.command == "provision":
                deps = ProvisionDeps(
                    store=store,
                    gremlin=gremlin,
                    ex_session=ex_cluster.connect(),
                    cell_session=cell_cluster.connect(),
                )
                mapping = provision_space(deps, args.space_id, tier=Tier(args.tier))
                print(f"[ok] space {mapping.space_id} 已开通（cell={mapping.cell_id}）")
                return 0
            if args.command == "migrate":
                mapping = store.get_space_mapping(args.space_id)
                source_ep = cell_host_endpoints(mapping.cell_id)
                target_id = args.to_cell
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
                try:
                    deps = MigrateDeps(
                        store=store,
                        source_gremlin=gremlin_client(source_ep["gremlin_url"]),
                        target_gremlin=gremlin_client(target_ep["gremlin_url"]),
                        source_cell_session=source_cluster.connect(),
                        target_cell_session=target_cluster.connect(),
                        source_es=es_client(source_ep["es_url"]),
                        target_es=es_client(target_ep["es_url"]),
                        ex_session=ex_cluster.connect(),
                        config=DEFAULT_CONFIG,
                    )
                    report = migrate_space(
                        deps,
                        args.space_id,
                        to_cell_id=args.to_cell,
                        grace_seconds=args.grace_seconds,
                    )
                    print(
                        f"[ok] space {report.space_id} 已迁移 {report.source_cell_id} → "
                        f"{report.target_cell_id}，只读窗口 "
                        f"{report.read_only_window_seconds}s（步骤耗时 {report.step_seconds}）"
                    )
                    return 0
                finally:
                    if target_cluster is not source_cluster:
                        target_cluster.shutdown()
                    source_cluster.shutdown()
            if args.command == "destroy":
                deps = DestroyDeps(
                    store=store,
                    gremlin=gremlin,
                    cell_session=cell_cluster.connect(),
                    ex_session=ex_cluster.connect(),
                    es=es_client(),
                    config=DEFAULT_CONFIG,
                    pulsar=pulsar_client(),  # 契约 5 广播通道（等 broker ack）
                )
                destroy_space(deps, args.space_id)
                print(f"[ok] space {args.space_id} 已注销，无残留")
                return 0
        finally:
            gremlin.close()
            ex_cluster.shutdown()
    finally:
        cell_cluster.shutdown()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
