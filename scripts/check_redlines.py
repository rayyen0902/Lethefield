"""M13 红线汇总核验（开发文档 §14）：红线 4/5/6 + Redis 豁免记录 + 红线 1/2/3 存在性核验。

静态部分（默认执行，进 CI 前段，不需要起栈）：
- 红线 4：`check_graph_config.static_check()`——deploy/janusgraph 配置不得显式设置
  `ids.authority.wait-time`（走默认值）。
- 红线 5：源码顺序校验——驱逐计算实例（removeConfiguration）必须先于 DROP KEYSPACE。
  destroy.py 按全文首个出现位置比对；migrate.py 按调用点比对（`_evict_graph(...)` 必须先于
  `_drop_graph_storage(...)`）——migrate.py 内 EX scratch 的 DROP KEYSPACE 不是
  JanusGraph keyspace，红线 5 管的是"在线 DROP JanusGraph 使用的 keyspace"。
- 红线 6（静态面）：ops/clock_monitor 模块可导入；运行时偏移巡检走 --runtime。
- Redis 豁免记录：docker-compose.yml 的 redis 服务必须开 AOF（appendonly yes +
  appendfsync everysec）且不配 maxmemory-policy 逐出；打印豁免键清单与论证（M13 定案：
  小键无逐出收益、ex:n 逐出破坏 n 分配——INCR 回退即序号重复分配风险、AOF 是权威值配套）。
- 红线 1/2/3 存在性：check_space_filter 扫描可执行且零违规；lethefield_rms.quota
  DEFAULT_QUOTA_CONFIG 存在；lethefield_fs.config.sweep_due 可导入且 SweepConfig 有
  cold_interval_seconds 字段。

--runtime 模式（需全栈，由集成测试调起，CI 静态段不跑）：时钟偏移巡检
（python -m lethefield_clock_monitor）+ check_graph_config 运行时校验，透传退出码。

用法：uv run python scripts/check_redlines.py [--runtime]
退出码：0 = 通过，1 = 发现违规。
"""

import argparse
import importlib.util
import re
import subprocess
import sys
from dataclasses import fields
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))  # 同目录巡检脚本 import（对齐 test_m1_infra.py 用法）

import check_graph_config  # noqa: E402
import check_space_filter  # noqa: E402

SCHEDULER = ROOT / "services/scheduler/src/lethefield_scheduler"

# Redis 豁免键清单（M13 定案记录：不配 maxmemory-policy 的论证对象）
REDIS_EXEMPT_KEYS = [
    ("ex:n:*", "space 级 n 权威计数（INCR 分配；逐出 = n 回退 = 序号重复分配风险）"),
    ("ex:last_write:*", "space 最近成功写入时间戳（DMS 新鲜度巡检数据源）"),
    ("fs:sweep:last_ok*", "sweep 心跳（全局 + per-space，liveness/分频判定依据）"),
    ("dms:*", "DMS 翻转边状态键（stale 状态去抖）"),
]
REDIS_EXEMPT_RATIONALE = (
    "Redis 逐出豁免论证（M13 定案）：上述均为小键（每 space 数十字节），逐出无内存收益；"
    "ex:n 被逐出会破坏 n 分配单调性（INCR 回退即序号重复分配，DMS n 一致性巡检兜底告警）；"
    "AOF（appendonly everysec）是权威值 ex:n 的持久化配套，纯 RDB 丢窗不可接受。"
)


def check_redline4() -> list[str]:
    """红线 4 静态面：deploy/janusgraph 配置文件不得显式设置 ids.authority.wait-time。"""
    return [f"红线 4：{f}" for f in check_graph_config.static_check()]


def check_redline5() -> list[str]:
    """红线 5 静态面：驱逐计算实例必须先于 DROP KEYSPACE（源码顺序校验）。"""
    failures: list[str] = []

    destroy = (SCHEDULER / "destroy.py").read_text(encoding="utf-8")
    evict_pos = destroy.find("removeConfiguration")
    drop = re.search(r"drop\s+keyspace", destroy, re.IGNORECASE)
    if evict_pos < 0 or drop is None or evict_pos > drop.start():
        failures.append(
            "红线 5：destroy.py 中 removeConfiguration（驱逐计算实例）"
            "未出现在首个 DROP KEYSPACE 之前"
        )

    migrate = (SCHEDULER / "migrate.py").read_text(encoding="utf-8")
    # 调用点比对：每个图存储 DROP（_drop_graph_storage 调用点）之前必须有驱逐调用
    # （_evict_graph 调用点）。排除 def 定义行本身。
    evict_calls = [m.start() for m in re.finditer(r"_evict_graph\(deps\.", migrate)]
    drop_calls = [m.start() for m in re.finditer(r"_drop_graph_storage\(deps\.", migrate)]
    if "removeConfiguration" not in migrate:
        failures.append("红线 5：migrate.py 缺少 removeConfiguration 驱逐逻辑")
    for pos in drop_calls:
        if not any(e < pos for e in evict_calls):
            failures.append(
                f"红线 5：migrate.py 偏移 {pos} 处 _drop_graph_storage 调用之前"
                "没有 _evict_graph 驱逐调用"
            )
    return failures


def check_redline6_static() -> list[str]:
    """红线 6 静态面：clock_monitor 模块存在且可导入（运行时巡检走 --runtime）。"""
    if importlib.util.find_spec("lethefield_clock_monitor") is None:
        return ["红线 6：ops/clock_monitor 模块不可导入（lethefield_clock_monitor 缺失）"]
    return []


def check_redis_exemption() -> list[str]:
    """Redis 豁免记录核验：AOF 配套必须在、逐出策略必须不在；打印键清单与论证。"""
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    match = re.search(r"^  redis:\n((?:    .*\n|\n)+)", text, re.MULTILINE)
    if match is None:
        return ["Redis 豁免核验：docker-compose.yml 找不到 redis 服务块"]
    # 去注释后再核验（块内注释会论证性提及 maxmemory-policy 等词）
    block = re.sub(r"#.*", "", match.group(1))
    failures = []
    if not re.search(r"appendonly[\",\s]+yes", block):
        failures.append("Redis 豁免核验：redis 服务缺 appendonly yes（AOF 是 ex:n 权威值配套）")
    if not re.search(r"appendfsync[\",\s]+everysec", block):
        failures.append("Redis 豁免核验：redis 服务缺 appendfsync everysec")
    if "maxmemory-policy" in block:
        failures.append("Redis 豁免核验：redis 服务配置了 maxmemory-policy 逐出（豁免键会被误伤）")
    print("Redis 逐出豁免记录（M13 定案）：")
    for pattern, desc in REDIS_EXEMPT_KEYS:
        print(f"  {pattern:<22} {desc}")
    print(f"  {REDIS_EXEMPT_RATIONALE}")
    return failures


def check_redline123_existence() -> list[str]:
    """红线 1/2/3 存在性核验：扫描器可执行零违规、配额与分频实现就位。"""
    failures = []
    violations = check_space_filter.scan()
    if violations:
        failures.append(f"红线 1：check_space_filter 扫描发现 {len(violations)} 条违规")
    try:
        from lethefield_rms import quota
    except ImportError as exc:
        failures.append(f"红线 2：lethefield_rms.quota 不可导入（{exc}）")
    else:
        if not hasattr(quota, "DEFAULT_QUOTA_CONFIG"):
            failures.append("红线 2：lethefield_rms.quota 缺 DEFAULT_QUOTA_CONFIG")
    try:
        from lethefield_fs.config import SweepConfig, sweep_due  # noqa: F401
    except ImportError as exc:
        failures.append(f"红线 3：lethefield_fs.config.sweep_due 不可导入（{exc}）")
    else:
        if "cold_interval_seconds" not in {f.name for f in fields(SweepConfig)}:
            failures.append("红线 3：SweepConfig 缺 cold_interval_seconds 字段（分频未落地）")
    return failures


def runtime_checks() -> list[str]:
    """--runtime：栈上跑时钟偏移巡检与 check_graph_config 运行时校验（透传退出码）。"""
    failures = []
    graph_config = str(ROOT / "scripts/check_graph_config.py")
    checks = [
        ("红线 6 运行时：时钟偏移巡检", [sys.executable, "-m", "lethefield_clock_monitor"]),
        ("红线 4 运行时：图配置生效值", [sys.executable, graph_config]),
    ]
    for name, cmd in checks:
        result = subprocess.run(cmd, cwd=ROOT)
        if result.returncode != 0:
            failures.append(f"{name}失败（退出码 {result.returncode}）")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="M13 红线汇总核验（静态默认；--runtime 需全栈）")
    parser.add_argument("--runtime", action="store_true", help="跑栈上运行时巡检（集成测试用）")
    args = parser.parse_args()

    checks = [
        ("红线 4（ids.authority.wait-time 默认）", check_redline4),
        ("红线 5（驱逐先于 DROP）", check_redline5),
        ("红线 6 静态面（clock_monitor 可导入）", check_redline6_static),
        ("Redis 逐出豁免记录", check_redis_exemption),
        ("红线 1/2/3 存在性", check_redline123_existence),
    ]
    failures: list[str] = []
    for name, fn in checks:
        result = fn()
        if result:
            failures.extend(result)
        else:
            print(f"[ok] {name}")
    if args.runtime:
        result = runtime_checks()
        if result:
            failures.extend(result)
        else:
            print("[ok] 运行时巡检（时钟偏移 + 图配置生效值）")

    if failures:
        print("M13 红线汇总核验失败：")
        for f in failures:
            print(f"  {f}")
        return 1
    print("M13 红线汇总核验通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
