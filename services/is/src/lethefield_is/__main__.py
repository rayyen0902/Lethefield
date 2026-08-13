"""CLI：python -m lethefield_is（M16 IS 简版，开发文档 §17）。

用法：
    python -m lethefield_is account create <account_id> [--name N]
    python -m lethefield_is account list
    python -m lethefield_is account disable <account_id>
    python -m lethefield_is space create <space_id> --account A [--tier cold|hot|premium]
    python -m lethefield_is space list --account A
    python -m lethefield_is credential issue --account A --actor-id X --space S [--space S2...]
        --scopes record,retrieve [--internal] [--ttl-seconds N]
    python -m lethefield_is credential revoke --jti J
    python -m lethefield_is credential list [--account A]
    python -m lethefield_is auth grant --space S --scopes calibration,content_copy
    python -m lethefield_is auth revoke --space S
    python -m lethefield_is auth list [--status active|revoked]
"""

import argparse

from lethefield_clients import (
    AuthRegistryStore,
    AuthScope,
    AuthStatus,
    CredentialStore,
    MappingTableControlPlaneStore,
    Tier,
    cassandra_cluster,
    ex_cassandra_cluster,
    gremlin_client,
    space_ref_of,
)
from lethefield_logschema import LogEvent, emit
from lethefield_scheduler.provision import ProvisionDeps, provision_space

from lethefield_is import tokens
from lethefield_is.service import create_space
from lethefield_is.store import IsStore


def _cmd_space_create(args) -> int:
    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    gremlin = gremlin_client()
    try:
        deps = ProvisionDeps(
            store=MappingTableControlPlaneStore(cell_cluster.connect()),
            gremlin=gremlin,
            ex_session=ex_cluster.connect(),
            cell_session=cell_cluster.connect(),
        )
        mapping = create_space(
            IsStore(),
            lambda space_id, tier: provision_space(deps, space_id, tier=tier),
            account_id=args.account,
            space_id=args.space_id,
            tier=Tier(args.tier),
        )
    finally:
        gremlin.close()
        ex_cluster.shutdown()
        cell_cluster.shutdown()
    emit(
        LogEvent(
            service="lethefield-is",
            event_type="space_created",
            space_id=mapping.space_id,
            payload={"account_id": args.account, "cell_id": mapping.cell_id, "tier": args.tier},
        ),
        sync=True,
    )
    print(f"[ok] space {mapping.space_id} 已开通并归属 {args.account}（cell={mapping.cell_id}）")
    return 0


def _cmd_credential_issue(args) -> int:
    token = tokens.issue_token(
        CredentialStore(),
        account_id=args.account,
        space_ids=args.space,
        agent_actor_id=args.actor_id,
        scopes=[s.strip() for s in args.scopes.split(",") if s.strip()],
        internal=args.internal,
        ttl_seconds=args.ttl_seconds,
    )
    emit(
        LogEvent(
            service="lethefield-is",
            event_type="credential_issued",
            payload={
                "account_id": args.account,
                "agent_actor_id": args.actor_id,
                "internal": args.internal,
            },
        ),
        sync=True,
    )
    print(token)
    return 0


def _cmd_credential_revoke(args) -> int:
    existed = CredentialStore().revoke(args.jti)
    if existed:
        emit(
            LogEvent(
                service="lethefield-is",
                event_type="credential_revoked",
                payload={"jti": args.jti},
            ),
            sync=True,
        )
    print("revoked" if existed else f"not found: {args.jti}")
    return 0 if existed else 1


def _cmd_auth_grant(args) -> int:
    space_ref = space_ref_of(args.space)
    AuthRegistryStore().grant(
        space_ref, [AuthScope(s.strip()) for s in args.scopes.split(",") if s.strip()]
    )
    print(f"granted: {args.space} scopes={args.scopes}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lethefield_is", description="IS 简版（M16）")
    sub = parser.add_subparsers(dest="group", required=True)

    p_account = sub.add_parser("account", help="账号 CRUD")
    account = p_account.add_subparsers(dest="command", required=True)
    p_acreate = account.add_parser("create", help="开户（幂等）")
    p_acreate.add_argument("account_id")
    p_acreate.add_argument("--name", default="", help="显示名")
    account.add_parser("list", help="账号列表")
    p_adisable = account.add_parser("disable", help="停用账号（签发侧拒签）")
    p_adisable.add_argument("account_id")

    p_space = sub.add_parser("space", help="空间创建入口（调 M9/M10 开通流水线）")
    space = p_space.add_subparsers(dest="command", required=True)
    p_screate = space.add_parser("create", help="开通 space 并登记账号归属")
    p_screate.add_argument("space_id")
    p_screate.add_argument("--account", required=True)
    p_screate.add_argument("--tier", choices=[t.value for t in Tier], default=Tier.COLD.value)
    p_slist = space.add_parser("list", help="账号名下 space 列表")
    p_slist.add_argument("--account", required=True)

    p_cred = sub.add_parser("credential", help="凭证签发与吊销")
    cred = p_cred.add_subparsers(dest="command", required=True)
    p_issue = cred.add_parser("issue", help="签发写入者凭证（每写入者身份单独签发）")
    p_issue.add_argument("--account", required=True)
    p_issue.add_argument("--actor-id", required=True, help="agent_actor_id（契约 3）")
    p_issue.add_argument("--space", action="append", required=True, help="授权 space，可多次")
    p_issue.add_argument("--scopes", required=True, help="逗号分隔（白名单见契约 3）")
    p_issue.add_argument(
        "--internal", action="store_true", help="内部签发渠道（debug scope 仅此渠道可授）"
    )
    p_issue.add_argument("--ttl-seconds", type=int, default=None, help="默认 24h（env 可配）")
    p_revoke = cred.add_parser("revoke", help="吊销凭证（吊销列表，立即生效）")
    p_revoke.add_argument("--jti", required=True)
    p_clist = cred.add_parser("list", help="凭证列表")
    p_clist.add_argument("--account", default=None)

    p_auth = sub.add_parser("auth", help="训练数据授权注册表入口（§12.4）")
    auth = p_auth.add_subparsers(dest="command", required=True)
    p_agrant = auth.add_parser("grant", help="登记授权（幂等）")
    p_agrant.add_argument("--space", required=True)
    p_agrant.add_argument("--scopes", required=True, help="逗号分隔：calibration,content_copy")
    p_arevoke = auth.add_parser("revoke", help="撤回授权（停止新增采样）")
    p_arevoke.add_argument("--space", required=True)
    p_alist = auth.add_parser("list", help="授权列表")
    p_alist.add_argument("--status", choices=[str(s) for s in AuthStatus], default=None)

    args = parser.parse_args(argv)

    if args.group == "account":
        store = IsStore()
        if args.command == "create":
            store.create_account(args.account_id, args.name)
            print(f"[ok] account {args.account_id} 已就绪")
            return 0
        if args.command == "list":
            for a in store.list_accounts():
                print(f"{a.account_id}\t{a.status}\t{a.display_name}")
            return 0
        if args.command == "disable":
            existed = store.disable_account(args.account_id)
            print("disabled" if existed else f"not found: {args.account_id}")
            return 0 if existed else 1

    if args.group == "space":
        if args.command == "create":
            return _cmd_space_create(args)
        if args.command == "list":
            for space_id in IsStore().list_spaces_of(args.account):
                print(space_id)
            return 0

    if args.group == "credential":
        if args.command == "issue":
            return _cmd_credential_issue(args)
        if args.command == "revoke":
            return _cmd_credential_revoke(args)
        if args.command == "list":
            for c in CredentialStore().list(args.account):
                print(
                    f"{c.jti}\t{c.account_id}\t{c.agent_actor_id}\t{c.status}\t"
                    f"scopes={','.join(c.scopes)}\tinternal={c.internal}"
                )
            return 0

    if args.group == "auth":
        if args.command == "grant":
            return _cmd_auth_grant(args)
        if args.command == "revoke":
            existed = AuthRegistryStore().revoke(space_ref_of(args.space))
            print("revoked" if existed else f"not found: {args.space}")
            return 0 if existed else 1
        if args.command == "list":
            store = AuthRegistryStore()
            status = AuthStatus(args.status) if args.status else None
            for entry in store.list(status):
                print(
                    f"{entry.space_ref}\t{entry.status}\t{','.join(str(s) for s in entry.scopes)}"
                )
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
