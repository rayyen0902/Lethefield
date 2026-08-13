"""运维操作面 CLI（M17，开发文档 §18）。

1.0 运维形态 = Grafana（读，M12）+ 本 CLI（写）+ 决策留痕（痕）。
全部人工触发点在此收口；每条命令强制绑定显式 space/cell（红线 1 操作面落实），
每条命令执行自动写决策留痕（audit.run_with_audit 统一包装）。

用法：
    python -m lethefield_ops_cli space status --space S [--space S2 ...]
    python -m lethefield_ops_cli space destroy --space S [--reason T]
    python -m lethefield_ops_cli space set-tier --space S --tier cold|hot|premium
    python -m lethefield_ops_cli migrate rebalance --space S
    python -m lethefield_ops_cli migrate to-cell --space S --to-cell C
    python -m lethefield_ops_cli migrate evacuate --cell C --space S [--space S2 ...]
    python -m lethefield_ops_cli auth revoke --space S
    python -m lethefield_ops_cli cell watermark --cell C [--refresh]
    python -m lethefield_ops_cli cell register --cell-id C --endpoint cassandra=H --endpoint es=H

全局参数：--operator 操作人（缺省 env LETHEFIELD_OPERATOR，再兜底 OS 用户）。
"""

import argparse
import sys

from lethefield_clients import (
    AuthRegistryStore,
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    es_client,
    gremlin_client,
)
from lethefield_rms.quota import QuotaCounters
from lethefield_training.config import TrainingConfig
from lethefield_training.hot_store import HotSampleStore

from lethefield_ops_cli import audit, commands


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lethefield_ops_cli", description="运维操作面 CLI（M17）：写操作统一入口，自动决策留痕"
    )
    parser.add_argument(
        "--operator", default=None, help="操作人（留痕字段；缺省 LETHEFIELD_OPERATOR / OS 用户）"
    )
    groups = parser.add_subparsers(dest="group", required=True)

    p_space = groups.add_parser("space", help="space 状态 / 销毁 / tier 调整")
    space = p_space.add_subparsers(dest="command", required=True)
    p_status = space.add_parser("status", help="space 状态查询（映射/tier/水位/配额用量）")
    p_status.add_argument("--space", action="append", required=True, help="可重复，显式列表")
    p_destroy = space.add_parser("destroy", help="整 space 销毁处置（M9/M10 注销流水线）")
    p_destroy.add_argument("--space", required=True)
    p_destroy.add_argument("--reason", default="", help="销毁事由（进决策留痕 rationale）")
    p_tier = space.add_parser("set-tier", help="tier 升降调整")
    p_tier.add_argument("--space", required=True)
    p_tier.add_argument("--tier", choices=[t.value for t in Tier], required=True)
    p_tier.add_argument("--reason", default="", help="调整事由（进决策留痕 rationale）")

    p_migrate = groups.add_parser("migrate", help="迁移触发（三类入口）")
    migrate = p_migrate.add_subparsers(dest="command", required=True)
    p_rebalance = migrate.add_parser("rebalance", help="再平衡：自动选水位最低的 open Cell")
    p_rebalance.add_argument("--space", required=True)
    p_rebalance.add_argument("--reason", default="")
    p_tocell = migrate.add_parser("to-cell", help="跨集群/指定目标迁移")
    p_tocell.add_argument("--space", required=True)
    p_tocell.add_argument("--to-cell", required=True)
    p_tocell.add_argument("--reason", default="")
    p_evacuate = migrate.add_parser(
        "evacuate", help="Cell 退役：显式 space 列表逐一迁出（无全局形态）"
    )
    p_evacuate.add_argument("--cell", required=True)
    p_evacuate.add_argument("--space", action="append", required=True, help="可重复，显式列表")
    p_evacuate.add_argument("--reason", default="")

    p_auth = groups.add_parser("auth", help="训练数据授权处置")
    auth = p_auth.add_subparsers(dest="command", required=True)
    p_revoke = auth.add_parser("revoke", help="授权撤回处置（M11：停新增 + 热层 scrub 清存量）")
    p_revoke.add_argument("--space", required=True)
    p_revoke.add_argument("--reason", default="", help="撤回事由（进决策留痕 rationale）")

    p_cell = groups.add_parser("cell", help="Cell 水位 / 新 Cell 筹备")
    cell = p_cell.add_subparsers(dest="command", required=True)
    p_watermark = cell.add_parser("watermark", help="Cell 水位查看（--refresh 现场探测刷新）")
    p_watermark.add_argument("--cell", required=True)
    p_watermark.add_argument("--refresh", action="store_true")
    p_register = cell.add_parser("register", help="新 Cell 筹备触发（映射表登记，幂等）")
    p_register.add_argument("--cell-id", required=True)
    p_register.add_argument(
        "--endpoint",
        action="append",
        required=True,
        help="k=v 形式，可重复；必需 cassandra=/es=（JanusGraph 容器视角）",
    )
    return parser


def _store(cluster) -> MappingTableControlPlaneStore:
    store = MappingTableControlPlaneStore(cluster.connect())
    store.ensure_tables()
    return store


def _parse_endpoints(pairs: list[str]) -> dict[str, str]:
    endpoints: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"--endpoint 需 k=v 形式：{pair!r}")
        key, _, value = pair.partition("=")
        endpoints[key.strip()] = value.strip()
    return endpoints


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    operator = audit.resolve_operator(args.operator)
    invoked = "ops: " + " ".join(argv if argv is not None else sys.argv[1:])
    reason = getattr(args, "reason", "")

    def run(decision: str, fn) -> int:
        return audit.run_with_audit(
            operator=operator, title=invoked, decision=decision, rationale=reason, fn=fn
        )

    cell_cluster = cassandra_cluster()
    store = _store(cell_cluster)
    try:
        if args.group == "space":
            if args.command == "status":

                def fn():
                    gremlin = gremlin_client()
                    try:
                        counters = QuotaCounters(gremlin, es_client())
                        return commands.cmd_space_status(store, counters, args.space)
                    finally:
                        gremlin.close()

                return run(f"查询 space 状态：{', '.join(args.space)}", fn)
            if args.command == "destroy":
                return run(
                    f"销毁 space {args.space}",
                    lambda: commands.cmd_destroy(store, cell_cluster, args.space),
                )
            if args.command == "set-tier":
                return run(
                    f"space {args.space} tier 调整为 {args.tier}",
                    lambda: commands.cmd_set_tier(store, args.space, Tier(args.tier)),
                )
        if args.group == "migrate":
            if args.command == "rebalance":
                return run(
                    f"再平衡迁移 space {args.space}",
                    lambda: commands.cmd_migrate_rebalance(store, args.space),
                )
            if args.command == "to-cell":
                return run(
                    f"迁移 space {args.space} 至 Cell {args.to_cell}",
                    lambda: commands.cmd_migrate_to_cell(store, args.space, args.to_cell),
                )
            if args.command == "evacuate":
                return run(
                    f"Cell {args.cell} 退役迁出：{', '.join(args.space)}",
                    lambda: commands.cmd_migrate_evacuate(store, args.cell, args.space),
                )
        if args.group == "auth" and args.command == "revoke":
            return run(
                f"撤回 space {args.space} 训练数据授权",
                lambda: commands.cmd_auth_revoke(
                    AuthRegistryStore(),
                    HotSampleStore(TrainingConfig.from_env().hot_root),
                    args.space,
                ),
            )
        if args.group == "cell":
            if args.command == "watermark":
                return run(
                    f"查看 Cell {args.cell} 水位" + ("（探测刷新）" if args.refresh else ""),
                    lambda: commands.cmd_cell_watermark(
                        store, args.cell, refresh=args.refresh, cell_cluster=cell_cluster
                    ),
                )
            if args.command == "register":
                return run(
                    f"登记新 Cell {args.cell_id}",
                    lambda: commands.cmd_cell_register(
                        store, args.cell_id, _parse_endpoints(args.endpoint)
                    ),
                )
    finally:
        cell_cluster.shutdown()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
