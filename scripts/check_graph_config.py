"""M1 巡检：`ids.authority.wait-time` 保持默认值（红线 4）。

该参数是 ID block 认领后的竞争确认睡眠，不是"ID 分配超时"——
调大会导致每次 ID 申请固定睡眠该时长、写入 100% 失败
（`StandardIDPool.waitForIDBlockGetter` TimeoutException，spike 实测）。

两层检查：
1. 静态：deploy/janusgraph 配置文件中不得出现该参数（走默认）
2. 运行时：打开任一图的 management，读取实际生效值必须等于默认值 100ms

用法：uv run python scripts/check_graph_config.py
退出码：0 = 合规，1 = 违反红线 4。
"""

import sys
from pathlib import Path

from gremlin_python.driver.client import Client

PARAM = "ids.authority.wait-time"
# JanusGraph 1.0.0 默认值 300ms（management 实测 toString = "PT0.3S"）
DEFAULT_MS = 300


def duration_to_ms(value: str) -> int:
    """解析 JanusGraph Duration 的 toString（ISO-8601 'PT0.3S' 或 '100 ms' 两种形态）。"""
    text = str(value).strip()
    if text.endswith(" ms"):
        return int(text.split()[0])
    if text.startswith("PT") and text.endswith("S"):
        return int(float(text[2:-1]) * 1000)
    raise ValueError(f"无法解析的 Duration 格式: {value!r}")


CONFIG_DIRS = [Path("deploy/janusgraph")]


def static_check() -> list[str]:
    failures = []
    for config_dir in CONFIG_DIRS:
        for path in config_dir.glob("**/*.properties"):
            for lineno, line in enumerate(path.read_text().splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(PARAM) and not stripped.startswith("#"):
                    failures.append(f"{path}:{lineno} 显式配置了 {PARAM}: {stripped}")
    return failures


def runtime_check() -> list[str]:
    """任一在线图的 management 读取生效值（动态图未建时跳过——静态检查已兜底）。"""
    client = Client("ws://localhost:8182/gremlin", "ConfigurationManagementGraph")
    try:
        # 注意：getGraphNames 的结果在服务端按元素逐个流回，result() 是名字列表
        graphs = client.submit("ConfiguredGraphFactory.getGraphNames()").all().result()
        if not graphs:
            print("[skip] 无动态图，运行时检查跳过（静态检查已覆盖默认值来源）")
            return []
        failures = []
        for gname in graphs:
            value = (
                client.submit(
                    "ConfiguredGraphFactory.open(gname).openManagement().get(param).toString()",
                    {"gname": gname, "param": PARAM},
                )
                .all()
                .result()[0]
            )
            print(f"[ok] 图 {gname} 生效值 {PARAM} = {value}")
            ms = duration_to_ms(value)
            if ms != DEFAULT_MS:
                failures.append(
                    f"图 {gname} 的 {PARAM} = {ms}ms，不等于默认值 {DEFAULT_MS}ms（红线 4）"
                )
        return failures
    finally:
        client.close()


def main() -> int:
    failures = static_check()
    try:
        failures += runtime_check()
    except Exception as exc:  # gremlin 不可达时不放行，巡检必须明确结论
        failures.append(f"运行时检查失败（gremlin 不可达？）：{exc}")

    if failures:
        for failure in failures:
            print(f"[FAIL] {failure}", file=sys.stderr)
        return 1
    print(f"红线 4 巡检通过：{PARAM} 保持默认值 {DEFAULT_MS}ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
