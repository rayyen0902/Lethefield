"""租户调度器 CLI（M9）。

用法：
    python -m lethefield_scheduler bootstrap                # 建控制面表 + 注册本地 Cell
    python -m lethefield_scheduler provision <space> [--tier cold|hot|premium]
    python -m lethefield_scheduler destroy <space>
    python -m lethefield_scheduler watermark [--cell ID]    # 探测并刷新水位状态
    python -m lethefield_scheduler export <file>            # 映射表备份（1.0 验收硬指标）
    python -m lethefield_scheduler restore <file>           # 从备份恢复（幂等）
    python -m lethefield_scheduler list                     # 列出 Cell 与 space 映射
"""

import argparse

from lethefield_clients import (
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    export_jsonl,
    gremlin_client,
    local_cell,
    restore_jsonl,
)

from lethefield_scheduler.config import DEFAULT_CONFIG
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_scheduler.watermark import refresh_cell


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
            print(f"[ok] 控制面就绪，Cell {local_cell().cell_id} 已注册")
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
            if args.command == "destroy":
                deps = DestroyDeps(
                    store=store,
                    gremlin=gremlin,
                    cell_session=cell_cluster.connect(),
                    ex_session=ex_cluster.connect(),
                    es=es_client(),
                    config=DEFAULT_CONFIG,
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
