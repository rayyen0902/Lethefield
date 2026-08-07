"""CLI：python -m lethefield_decision_log submit|get|list"""

import argparse

from lethefield_clients import (
    DECISION_OUTCOMES,
    ESCALATION_TYPES,
    make_feed_publisher,
    pulsar_client,
)

from lethefield_decision_log import DecisionLogStore


def main() -> None:
    parser = argparse.ArgumentParser(prog="decision_log", description="决策留痕表单")
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit", help="提交一条决策留痕")
    p_submit.add_argument("--title", required=True)
    p_submit.add_argument("--decision", required=True)
    p_submit.add_argument("--decided-by", required=True)
    p_submit.add_argument("--context", default="")
    p_submit.add_argument("--rationale", default="")
    p_submit.add_argument("--agent-suggestion", default="", help="Agent 建议内容（§11.3）")
    p_submit.add_argument(
        "--outcome",
        default="accepted",
        choices=sorted(DECISION_OUTCOMES),
        help="人类对建议的处置结果（≠accepted 触发 R1）",
    )
    p_submit.add_argument(
        "--escalation-type",
        default=None,
        choices=sorted(ESCALATION_TYPES),
        help="§11.2 升级四类（非空触发 R2）",
    )
    p_submit.add_argument(
        "--feed",
        action="store_true",
        help="R1/R2 命中时发布训练 feed（M11 入料口 ①；需 Pulsar 在线）",
    )

    p_get = sub.add_parser("get", help="按 id 查询")
    p_get.add_argument("id", type=int)

    p_list = sub.add_parser("list", help="最近记录列表")
    p_list.add_argument("--limit", type=int, default=50)

    args = parser.parse_args()

    publish = None
    if args.command == "submit" and args.feed:
        publish = make_feed_publisher(pulsar_client())
    store = DecisionLogStore(publish=publish)

    if args.command == "submit":
        record_id = store.submit(
            title=args.title,
            decision=args.decision,
            decided_by=args.decided_by,
            context=args.context,
            rationale=args.rationale,
            agent_suggestion=args.agent_suggestion,
            outcome=args.outcome,
            escalation_type=args.escalation_type,
        )
        print(f"submitted: id={record_id}")
    elif args.command == "get":
        record = store.get(args.id)
        print(record if record else f"not found: id={args.id}")
    elif args.command == "list":
        for record in store.list(args.limit):
            print(record)


if __name__ == "__main__":
    main()
