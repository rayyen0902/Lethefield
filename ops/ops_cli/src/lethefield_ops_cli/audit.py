"""M17 决策留痕包装（开发文档 §18 硬约束 2）：每条命令执行自动写决策留痕，
无留痕能力的命令不得上线。

执行序列（单点，所有命令统一走 `run_with_audit`）：
1. PG 预检（留痕库可达）——不可达直接拒绝执行（fail-closed，退出码 1）。
   处置类操作（销毁/撤回）因此满足"处置与留痕是原子要求"：留痕库挂了的
   操作不会被发起。
2. 执行业务函数，捕获结果。命令入口在预检前只建立存储连接（ensure_tables 是
   幂等引导 DDL，scheduler CLI 同款，无业务副作用）；所有业务写都在预检之后。
3. 提交决策留痕（操作人/命令/参数/结果）。outcome 恒 "accepted"——
   DECISION_OUTCOMES 语义是"人类对建议的处置结果"（§11.3），ops 决策无
   agent 建议，不拉伸枚举语义；执行成败记 context.result。
4. 业务已执行但留痕提交失败 → 退出码 2 + stderr 提示人工补录（不静默）。
"""

import getpass
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field

from lethefield_clients import pg_connection
from lethefield_decision_log import DecisionLogStore

# 退出码约定：0 成功；1 业务失败/预检拒绝；2 业务已执行但留痕写入失败（需人工补录）
EXIT_AUDIT_WRITE_FAILED = 2


@dataclass(frozen=True)
class CommandResult:
    """业务函数返回：退出码 + 留痕结果摘要 + 人类可读输出行（由包装层统一打印）。"""

    exit_code: int
    detail: str
    lines: tuple[str, ...] = field(default_factory=tuple)


def resolve_operator(explicit: str | None) -> str:
    """操作人解析：--operator 显式 > env LETHEFIELD_OPERATOR > OS 用户。"""
    return explicit or os.environ.get("LETHEFIELD_OPERATOR") or getpass.getuser()


def _precheck(dsn: str | None) -> None:
    """留痕库可达性预检（fail-closed）：不可达抛异常，调用方拒绝执行。"""
    with pg_connection(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")


def run_with_audit(
    *,
    operator: str,
    title: str,
    decision: str,
    fn: Callable[[], CommandResult],
    rationale: str = "",
    store: DecisionLogStore | None = None,
    dsn: str | None = None,
) -> int:
    """留痕包装：预检 → 执行 → 留痕。返回进程退出码。"""
    store = store if store is not None else DecisionLogStore(dsn)
    try:
        _precheck(dsn)
    except Exception as e:
        print(f"[拒绝执行] 决策留痕库不可达（无留痕能力的命令不得上线）：{e}", file=sys.stderr)
        return 1

    try:
        result = fn()
    except Exception as e:
        result = CommandResult(1, f"{type(e).__name__}: {e}")

    context = json.dumps(
        {"result": "ok" if result.exit_code == 0 else "error", "detail": result.detail},
        ensure_ascii=False,
    )
    try:
        record_id = store.submit(
            title=title,
            decision=decision,
            decided_by=operator,
            context=context,
            rationale=rationale,
            outcome="accepted",
        )
    except Exception as e:
        print(
            f"[警告] 操作已执行但决策留痕写入失败，需人工补录（结果：{result.detail}）：{e}",
            file=sys.stderr,
        )
        return EXIT_AUDIT_WRITE_FAILED

    for line in result.lines:
        print(line)
    if result.exit_code == 0:
        print(f"[ok] {result.detail}（留痕 #{record_id}，操作人 {operator}）")
    else:
        print(f"[失败] {result.detail}（留痕 #{record_id}，操作人 {operator}）", file=sys.stderr)
    return result.exit_code
