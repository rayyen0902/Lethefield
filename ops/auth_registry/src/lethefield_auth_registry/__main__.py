"""CLI：python -m lethefield_auth_registry grant|revoke|get|list|check"""

import argparse

from lethefield_auth_registry import AuthRegistryStore, AuthScope, AuthStatus


def main() -> None:
    parser = argparse.ArgumentParser(prog="auth_registry", description="训练数据授权注册表")
    sub = parser.add_subparsers(dest="command", required=True)

    p_grant = sub.add_parser("grant", help="登记授权")
    p_grant.add_argument("space_ref")
    p_grant.add_argument("--scopes", nargs="+", required=True, choices=[str(s) for s in AuthScope])

    p_revoke = sub.add_parser("revoke", help="撤回授权")
    p_revoke.add_argument("space_ref")

    p_get = sub.add_parser("get", help="查询单条")
    p_get.add_argument("space_ref")

    p_list = sub.add_parser("list", help="列表")
    p_list.add_argument("--status", choices=[str(s) for s in AuthStatus], default=None)

    p_check = sub.add_parser("check", help="授权拦截判定")
    p_check.add_argument("space_ref")
    p_check.add_argument("--scope", required=True, choices=[str(s) for s in AuthScope])

    args = parser.parse_args()
    store = AuthRegistryStore()

    if args.command == "grant":
        store.grant(args.space_ref, [AuthScope(s) for s in args.scopes])
        print(f"granted: {args.space_ref} scopes={args.scopes}")
    elif args.command == "revoke":
        existed = store.revoke(args.space_ref)
        print("revoked" if existed else f"not found: {args.space_ref}")
    elif args.command == "get":
        entry = store.get(args.space_ref)
        print(entry if entry else f"not found: {args.space_ref}")
    elif args.command == "list":
        for entry in store.list(AuthStatus(args.status) if args.status else None):
            print(entry)
    elif args.command == "check":
        authorized = store.is_authorized(args.space_ref, AuthScope(args.scope))
        print("authorized" if authorized else "rejected")


if __name__ == "__main__":
    main()
