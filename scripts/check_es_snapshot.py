"""ES 快照存在性巡检（修订记录第 25 条配套，M17 操作面范围）。

rms_vectors 是检索面向量的唯一载体（embedding 不可重放，热节点向量灾难恢复
= ES 快照/备份运维前提）。本巡检验证：
1. 快照仓库已注册且可用；
2. 最近一次快照成功（state=SUCCESS）且龄期不超 --max-age-hours。

退出码 1 = 告警（接 DMS/cron 巡检节奏）；集群级运维项，无 space 维度。

用法：uv run python scripts/check_es_snapshot.py [--repo lethefield_backup] [--max-age-hours 48]
"""

import argparse
import sys
from datetime import UTC, datetime

from lethefield_clients import es_client

DEFAULT_REPO = "lethefield_backup"


def main() -> int:
    parser = argparse.ArgumentParser(prog="check_es_snapshot", description="ES 快照存在性巡检")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="快照仓库名")
    parser.add_argument("--max-age-hours", type=float, default=48.0, help="最近快照最大龄期")
    args = parser.parse_args()

    es = es_client()
    try:
        repos = es.snapshot.get_repository(name=args.repo)
    except Exception:
        print(f"[告警] 快照仓库 {args.repo} 未注册或不可用——检索面灾难恢复无保障")
        return 1
    print(f"[ok] 仓库 {args.repo} 已注册：{repos[args.repo]['type']}")

    snaps = es.snapshot.get(repository=args.repo, snapshot="_all")["snapshots"]
    ok_snaps = [s for s in snaps if s["state"] == "SUCCESS"]
    if not ok_snaps:
        print(f"[告警] 仓库 {args.repo} 无成功快照（共 {len(snaps)} 份）")
        return 1
    latest = max(ok_snaps, key=lambda s: s["start_time_in_millis"])
    age_hours = (datetime.now(UTC).timestamp() * 1000 - latest["start_time_in_millis"]) / 3_600_000
    print(
        f"[ok] 最新成功快照 {latest['snapshot']}，龄期 {age_hours:.1f}h"
        f"（上限 {args.max_age_hours}h），索引 {latest['indices']}"
    )
    if age_hours > args.max_age_hours:
        print(f"[告警] 最新快照龄期 {age_hours:.1f}h 超上限 {args.max_age_hours}h")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
