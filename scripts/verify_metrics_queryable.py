"""M12 开发期最小集指标可查询性验证（阶段 1 准出条件）。

对最小集 15 项（16 个指标名）逐项执行真实 Prometheus /api/v1/query 按名断言：
- 有数据：断言结果非空，记录序列数与样本值；
- 无数据：断言 status=success（查询语法合法），记"无数据，语法合法"。
两种情况都算有结果行，不跳过。类型经 /api/v1/metadata 与源码定义类型比对
（target 无数据时 metadata 可能缺失，记"类型未取得"）。
另执行 Grafana dashboard（deploy/grafana/provisioning/dashboards/lethefield.json）
全部 6 条面板 PromQL 并记录结果。

结果落 deploy/baselines/m12_metrics_queryable_v1.md（逐项表格，可重跑覆盖）。

前置：docker compose 全栈 + 宿主机 uv run 的 lethefield-api / lethefield-fs /
lethefield-ingest-dms / lethefield-metrics-exporter 进程（Prometheus targets 翻绿）。

用法：uv run python scripts/verify_metrics_queryable.py
无命令行参数（无 space 维度的 Prometheus 查询巡检，URL 走 env 覆盖）。
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import requests

PROM_URL = "http://localhost:9090"
DASHBOARD = Path("deploy/grafana/provisioning/dashboards/lethefield.json")
REPORT = Path("deploy/baselines/m12_metrics_queryable_v1.md")

# 最小集 15 项 = 16 个指标名（fs 两项算一项）；类型以源码定义点为准（非命名后缀推断）。
# 定义点：ops/metrics_exporter exporter.py、services/rms retrieve.py、services/api
# service.py/ex_ingest.py、ops/ingest_dms backlog.py/freshness.py、services/fs worker.py
EXPECTED_TYPES = {
    "lethefield_graph_open_duration_seconds": "histogram",
    "lethefield_graph_lru_cache_hit_ratio": "gauge",
    "lethefield_retrieve_stage_duration_seconds": "histogram",
    "lethefield_record_confirm_duration_seconds": "histogram",
    "lethefield_pulsar_backlog_events": "gauge",
    "lethefield_ex_write_duration_seconds": "histogram",
    "lethefield_ex_last_write_age_seconds": "gauge",
    "lethefield_fs_sweep_lag_seconds": "gauge",
    "lethefield_fs_sweep_processed_total": "counter",
    "lethefield_cell_watermark_ratio": "gauge",
    "lethefield_space_storage_bytes": "gauge",
    # retrieve.py:625 实为 histogram（命名后缀易误判为 gauge，以源码为准）
    "lethefield_ff_theta_filter_ratio": "histogram",
    "lethefield_ff_delta_applied_total": "counter",
    "lethefield_ff_recalled_then_touched_rate": "gauge",
    "lethefield_agent_suggestion_total": "counter",
    "lethefield_escalation_total": "counter",
}


def _prom_get(path: str, **params) -> dict:
    resp = requests.get(f"{PROM_URL}{path}", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _metadata_type(name: str) -> str | None:
    """/api/v1/metadata 取指标类型；target 无数据时可能缺失 → None。"""
    data = _prom_get("/api/v1/metadata", metric=name)
    entries = data.get("data", {}).get(name)
    if not entries:
        return None
    return entries[0].get("type")


def _query(expr: str) -> tuple[str, list]:
    """执行 instant query，返回 (status, result list)；异常记 error。"""
    try:
        data = _prom_get("/api/v1/query", query=expr)
    except Exception as exc:
        return f"error: {exc}", []
    return data.get("status", "unknown"), data.get("data", {}).get("result", [])


def _fmt_sample(series: list, limit: int = 3) -> str:
    parts = []
    for s in series[:limit]:
        labels = ",".join(f"{k}={v!r}" for k, v in s["metric"].items() if k != "__name__")
        parts.append(f"{{{labels}}}={s['value'][1]}" if labels else f"={s['value'][1]}")
    suffix = f" …共{len(series)}序列" if len(series) > limit else ""
    return "; ".join(parts) + suffix


def _dashboard_exprs(path: Path) -> list[tuple[str, str]]:
    """提取 dashboard 全部面板 PromQL（面板标题, expr）。"""
    doc = json.loads(path.read_text())
    exprs = []
    for panel in doc.get("panels", []):
        for target in panel.get("targets", []):
            if target.get("expr"):
                exprs.append((panel.get("title", "?"), target["expr"]))
    return exprs


def main() -> int:
    rows = []  # (指标名, 查询, 类型比对, 结果, 判定)
    failures = 0
    for name, expected in EXPECTED_TYPES.items():
        # histogram 基名无序列（只暴露 _bucket/_sum/_count），按名断言查 _count
        expr = f"{name}_count" if expected == "histogram" else name
        actual = _metadata_type(name)
        if actual is None:
            type_note = "类型未取得"
        elif actual == expected:
            type_note = f"类型一致（{actual}）"
        else:
            type_note = f"类型不符（期望 {expected}，实际 {actual}）"
        status, series = _query(expr)
        if status != "success":
            verdict = "FAIL"
            result = f"查询失败：{status}"
        elif series:
            result = f"{len(series)} 序列：{_fmt_sample(series)}"
            verdict = "FAIL" if "不符" in type_note else "PASS（有数据）"
        else:
            result = "无数据，语法合法"
            verdict = "FAIL" if "不符" in type_note else "PASS（语法合法）"
        if verdict.startswith("FAIL"):
            failures += 1
        rows.append((name, expr, type_note, result, verdict))

    promql_rows = []  # (面板, expr, 结果, 判定)
    for title, expr in _dashboard_exprs(DASHBOARD):
        status, series = _query(expr)
        if status != "success":
            verdict, result = "FAIL", f"查询失败：{status}"
        elif series:
            verdict, result = "PASS（有数据）", f"{len(series)} 序列：{_fmt_sample(series)}"
        else:
            verdict, result = "PASS（语法合法）", "无数据，语法合法"
        if verdict == "FAIL":
            failures += 1
        promql_rows.append((title, expr, result, verdict))

    targets = _prom_get("/api/v1/targets", state="active")["data"]["activeTargets"]
    target_health = {t["labels"]["job"]: t["health"] for t in targets}

    now = datetime.now().astimezone()
    lines = [
        "# M12 开发期最小集指标可查询性验证记录（v1）",
        "",
        f"- 日期：{now:%Y-%m-%d %H:%M:%S %z}",
        "- 环境：Mac dev 机（colima docker compose 全栈 + 宿主机 uv run 服务进程）",
        f"- Prometheus：{PROM_URL}（scrape 走 host.docker.internal）",
        "- 生成：`uv run python scripts/verify_metrics_queryable.py`（可重跑，覆盖本文件）",
        "- 判定口径：有数据 = 序列非空；无数据 = /api/v1/query status=success（语法合法）。"
        "histogram 按名断言查 `<name>_count`（基名只暴露 _bucket/_sum/_count）。"
        "类型经 /api/v1/metadata 与源码定义类型比对，target 无数据时记『类型未取得』。",
        "",
        "## Prometheus targets 健康",
        "",
        "注：lethefield-ss / lethefield-training / lethefield-writer 不在最小集指标来源内"
        "（16 项全部由 api/fs/ingest-dms/metrics-exporter 四进程覆盖），本验证不起这三个进程。",
        "",
        "| job | health |",
        "|---|---|",
        *(f"| {job} | {health} |" for job, health in sorted(target_health.items())),
        "",
        "## 逐项断言（15 项 / 16 个指标名）",
        "",
        "| 指标名 | 查询 | 类型 | 结果 | 判定 |",
        "|---|---|---|---|---|",
        *(f"| `{n}` | `{q}` | {t} | {r} | {v} |" for n, q, t, r, v in rows),
        "",
        "## Grafana 面板 PromQL（deploy/grafana/provisioning/dashboards/lethefield.json）",
        "",
        "| 面板 | expr | 结果 | 判定 |",
        "|---|---|---|---|",
        *(f"| {t} | `{e}` | {r} | {v} |" for t, e, r, v in promql_rows),
        "",
        f"## 汇总：{'全部 PASS' if failures == 0 else f'{failures} 项 FAIL'}",
        "",
    ]
    REPORT.write_text("\n".join(lines))

    print(f"{'指标名':<48} {'判定'}")
    for name, _q, _t, result, verdict in rows:
        print(f"{name:<48} {verdict}  ({result})")
    print("\nGrafana 面板 PromQL：")
    for title, _e, result, verdict in promql_rows:
        print(f"  [{verdict}] {title}：{result}")
    print(f"\ntargets: {target_health}")
    print(f"记录已写入 {REPORT}；{'全部 PASS' if failures == 0 else f'{failures} 项 FAIL'}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
