"""M8 巡检：记忆空间模型的两条静态红线（开发文档 §9 验收标准）。

1. **无 agent_id 分区键残留**：代码库 .py 中不存在独立词 `agent_id`
   （`agent_actor_id` 是合法的写入者身份属性，不算残留——子串层面本就不包含
   `agent_id`，这里用词边界正则双保险）。
2. **核心服务无 space_type 分支**：`space_type` 只允许出现在 `libs/clients`
   （spaces.py 枚举定义 / control_plane.py 注解字段）与本巡检脚本自身；
   services/ ops/ tests/ 及其他 libs 一律禁止引用——它是产品/运营标注，
   不得影响 RMS/FS/SS 核心逻辑（设计文档 §8）。

纯静态扫描，不需要起栈。用法：uv run python scripts/check_space_model.py
退出码：0 = 通过，1 = 发现违规。
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCAN_DIRS = ["libs", "ops", "services", "scripts", "tests"]

# 独立词 agent_id（前后非标识符字符）
AGENT_ID_RE = re.compile(r"(?<![A-Za-z0-9_])agent_id(?![A-Za-z0-9_])")
SPACE_TYPE_RE = re.compile(r"space_type")

# space_type 合法出现范围：libs/clients 内（定义与注解）+ 本脚本
SPACE_TYPE_ALLOWED = re.compile(r"^(libs/clients/|scripts/check_space_model\.py$)")


def scan() -> list[str]:
    failures: list[str] = []
    for d in SCAN_DIRS:
        base = ROOT / d
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.py")):
            rel = path.relative_to(ROOT).as_posix()
            if rel == "scripts/check_space_model.py":
                continue  # 本脚本必然提及被巡检的词，自我豁免
            text = path.read_text(encoding="utf-8")
            for lineno, line in enumerate(text.splitlines(), start=1):
                if AGENT_ID_RE.search(line):
                    failures.append(
                        f"{rel}:{lineno}: 发现独立词 agent_id（分区键必须统一为 space_id）"
                    )
                if SPACE_TYPE_RE.search(line) and not SPACE_TYPE_ALLOWED.match(rel):
                    failures.append(
                        f"{rel}:{lineno}: 核心服务/测试引用 space_type"
                        "（仅允许 libs/clients 定义注解，禁止业务分支）"
                    )
    return failures


def main() -> int:
    failures = scan()
    if failures:
        print("M8 空间模型巡检失败：")
        for f in failures:
            print(f"  {f}")
        return 1
    print("M8 空间模型巡检通过：无 agent_id 分区键残留；核心服务无 space_type 分支。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
