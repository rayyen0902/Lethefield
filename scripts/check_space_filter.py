"""M13 红线 1 巡检：静态拦截"无 space 纪律"的数据面访问（开发文档 §14，设计文档 §11.5）。

三条规则（AST 级别，不需要起栈）：

- **规则 A（图遍历必须带 space 过滤）**：gremlin 脚本字符串常量含 `.V(` / `.E(` 的，
  同串必须含 `has('space_id'` / `has("space_id"`；纯计数遍历 `.V().count()` /
  `.E().count()` 豁免（per-space 图计数，图名即 space）。
- **规则 B（跨 space/集群级调用必须登记）**：`getGraphNames` / `size_estimates` /
  `indices.stats` / `list_spaces` / `list_space_mappings` / `list_cells` /
  `lethefield-logs` 读取，所在文件必须出现 `@redline1_exempt` 装饰器，或命中
  下方 BUILTIN_EXEMPTIONS。libs/clients 是抽象定义处，天然豁免。
- **规则 C（入口纪律）**：ops/ services/ scripts/ 下含 `argparse.ArgumentParser` 的
  文件，必须有 `--space`/`--spaces` 可选参数或 `space_id`/`space`/`gname`/`graph`/
  `space_ref` 位置参数，否则须 `@redline1_exempt` 登记或进内置豁免表。

扫描范围：规则 A/B = libs/ ops/ services/（排除 */tests/*）；规则 C 另含 scripts/。
豁免必须机器可读（装饰器或本表），注释不构成豁免。

用法：uv run python scripts/check_space_filter.py
退出码：0 = 通过，1 = 发现违规。
"""

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS_AB = ["libs", "ops", "services"]
SCAN_DIRS_C = ["libs", "ops", "services", "scripts"]
SELF = "scripts/check_space_filter.py"

# ---------------------------------------------------------------------------
# 内置豁免表（可审查清单）：相对路径 → 理由。
# 每进表一条必须经过评审并写明理由；常驻 worker 优先打 @redline1_exempt
# 登记（豁免三要件：枚举走 list_spaces、逐 space 独立处理、批间节流），
# 本表留给控制面/集群级巡检等"无 space 维度"的合法入口。
# ---------------------------------------------------------------------------
BUILTIN_EXEMPTIONS = {
    # —— 规则 B：控制面/管理面操作（逐 space gname 上下文或纯元数据，非数据面扫描）
    "services/scheduler/src/lethefield_scheduler/destroy.py": (
        "控制面注销流水线：getGraphNames 用于单 space gname 的驱逐与无残留校验"
    ),
    "services/scheduler/src/lethefield_scheduler/provision.py": (
        "控制面开通流水线：getGraphNames 做单 space gname 幂等建图判断"
    ),
    "services/scheduler/src/lethefield_scheduler/migrate.py": (
        "控制面迁移流水线：getGraphNames 用于单 space gname 的驱逐/回滚"
    ),
    "services/scheduler/src/lethefield_scheduler/__main__.py": (
        "控制面元数据 CLI：list/watermark 子命令列 Cell 与映射（映射表本身即枚举源）"
    ),
    "services/scheduler/src/lethefield_scheduler/watermark.py": (
        "控制面水位探测：list_cells 枚举 Cell 元数据（容量标注，非业务数据面）"
    ),
    "services/rms/src/lethefield_rms/schema.py": (
        "图 schema 管理 API：getGraphNames 做幂等建图判断（管理面，逐 gname 调用）"
    ),
    # —— 规则 C：集群级巡检 / 无 space 维度的入口
    "ops/clock_monitor/src/lethefield_clock_monitor/__main__.py": (
        "集群级时钟偏移巡检（红线 6）：比对组件时钟，无 space 维度"
    ),
    "services/fs/src/lethefield_fs/liveness.py": (
        "集群级 DMS 巡检：只读全局心跳键 fs:sweep:last_ok，无 space 维度"
    ),
    "services/scheduler/src/lethefield_scheduler/training_control_sink.py": (
        "Pulsar 控制 topic 消费者（契约 5）：消息自带 space，无枚举无扫描"
    ),
    "ops/decision_log/src/lethefield_decision_log/__main__.py": (
        "决策留痕表单：Postgres 运维元数据，无 space 维度"
    ),
    "scripts/check_rms_schema.py": (
        "必填 --graph 单图巡检（图名即 space，入口已显式收敛到单 space）"
    ),
    "scripts/check_redlines.py": "红线汇总核验器：集群级核验入口，无 space 维度",
}

# 规则 B：调用名触发集合（dotted 末段命中即触发；indices.stats 按完整后缀判定）
_CALL_NAME_TRIGGERS = {"getGraphNames", "list_spaces", "list_space_mappings", "list_cells"}
# 规则 B：字符串触发——getGraphNames 出现在任意字符串常量（gremlin 脚本载体），
# size_estimates / lethefield-logs 只认调用内字符串（避开 docstring 论证性提及）
_STR_ANYWHERE_TRIGGERS = ("getGraphNames",)
_STR_IN_CALL_TRIGGERS = ("size_estimates", "lethefield-logs")

# 规则 A 豁免：纯计数遍历（per-space 图计数合法，图名即 space）
_COUNT_ONLY_RE = re.compile(r"\.[VE]\(\)\s*\.count\(\)")
_SPACE_FILTER_MARKERS = ("has('space_id'", 'has("space_id"')

# 规则 C：收敛口形态
_SPACE_OPTIONALS = {"--space", "--spaces"}
_SPACE_POSITIONALS = {"space_id", "space", "gname", "graph", "space_ref"}


def _dotted(node: ast.expr) -> str:
    """属性链/名字还原点分名（如 deps.store.list_spaces / es.indices.stats）。"""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _has_exempt_decorator(tree: ast.Module) -> bool:
    """文件内是否存在 @redline1_exempt 装饰器（AST 识别，注释不算）。"""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            for dec in node.decorator_list:
                target = dec.func if isinstance(dec, ast.Call) else dec
                if isinstance(target, (ast.Name, ast.Attribute)) and _dotted(target).endswith(
                    "redline1_exempt"
                ):
                    return True
    return False


def _rule_a(tree: ast.Module, rel: str) -> list[str]:
    failures = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        text = node.value
        if ".V(" not in text and ".E(" not in text:
            continue
        rest = _COUNT_ONLY_RE.sub("", text)
        if ".V(" not in rest and ".E(" not in rest:
            continue  # 纯计数遍历豁免
        if not any(marker in text for marker in _SPACE_FILTER_MARKERS):
            failures.append(
                f"{rel}:{node.lineno}: 规则 A：图遍历缺 space 过滤"
                "（含 .V(/ .E( 的脚本同串必须 has('space_id'；纯 .V().count()/.E().count() 豁免）"
            )
    return failures


def _rule_b(tree: ast.Module, rel: str) -> list[str]:
    if rel.startswith("libs/clients/"):
        return []  # 抽象定义处天然豁免
    if rel in BUILTIN_EXEMPTIONS:
        return []
    if _has_exempt_decorator(tree):
        return []
    failures = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            dotted = _dotted(node.func)
            last = dotted.rsplit(".", 1)[-1]
            if last in _CALL_NAME_TRIGGERS or dotted.endswith("indices.stats"):
                failures.append(
                    f"{rel}:{node.lineno}: 规则 B：跨 space/集群级调用 {dotted}() "
                    "未登记（需 @redline1_exempt 或内置豁免表）"
                )
                continue
            for sub in ast.walk(node):
                if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                    hit = next(
                        (t for t in _STR_IN_CALL_TRIGGERS if t in sub.value),
                        None,
                    )
                    if hit:
                        failures.append(
                            f"{rel}:{sub.lineno}: 规则 B：集群级读取（{hit}）"
                            "未登记（需 @redline1_exempt 或内置豁免表）"
                        )
                        break
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if any(t in node.value for t in _STR_ANYWHERE_TRIGGERS):
                failures.append(
                    f"{rel}:{node.lineno}: 规则 B：集群级操作 getGraphNames "
                    "未登记（需 @redline1_exempt 或内置豁免表）"
                )
    return failures


def _rule_c(tree: ast.Module, rel: str) -> list[str]:
    if rel in BUILTIN_EXEMPTIONS:
        return []
    has_parser = any(
        isinstance(node, ast.Call) and _dotted(node.func).endswith("ArgumentParser")
        for node in ast.walk(tree)
    )
    if not has_parser:
        return []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _dotted(node.func).endswith("add_argument")):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
            continue
        name = first.value
        if name in _SPACE_OPTIONALS or (not name.startswith("-") and name in _SPACE_POSITIONALS):
            return []  # 有 space 收敛口
    if _has_exempt_decorator(tree):
        return []
    return [
        f"{rel}: 规则 C：入口缺 space 收敛口（--space(s) 可选参数或 space_id/space/gname/"
        "graph/space_ref 位置参数），常驻 worker 须 @redline1_exempt 登记或进内置豁免表"
    ]


def _iter_py(dirs: list[str]):
    for d in dirs:
        base = ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(ROOT).as_posix()
            if rel == SELF or "tests" in path.relative_to(ROOT).parts:
                continue
            yield rel, path


def scan() -> list[str]:
    failures: list[str] = []
    for rel, path in _iter_py(SCAN_DIRS_C):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            failures.append(f"{rel}: 无法解析（{exc}）")
            continue
        if any(rel.startswith(f"{d}/") for d in SCAN_DIRS_AB):
            failures.extend(_rule_a(tree, rel))
            failures.extend(_rule_b(tree, rel))
        failures.extend(_rule_c(tree, rel))
    return failures


def main() -> int:
    failures = scan()
    if failures:
        print("M13 红线 1 巡检失败：")
        for f in failures:
            print(f"  {f}")
        return 1
    print("M13 红线 1 巡检通过：图遍历均带 space 过滤；跨 space/集群级调用均已登记；入口纪律合规。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
