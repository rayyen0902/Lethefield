"""CLI：python -m lethefield_ss worker|smoke|validate（M14 SS 显著性打分服务）"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

from lethefield_clients import (
    MappingTableControlPlaneStore,
    cassandra_cluster,
    ex_cassandra_cluster,
    pulsar_client,
)
from lethefield_clients.ex_stream import DIMENSIONS, ExStreamEvent
from lethefield_clients.redline import redline1_exempt
from lethefield_logschema import configure as logschema_configure
from lethefield_metrics import metrics_port_from_env, start_metrics_server

from lethefield_ss import worker
from lethefield_ss.config import SSConfig
from lethefield_ss.llm import LLMScorer, ScoringError
from lethefield_ss.scoring import score_event

# ss worker /metrics 暴露口默认端口（M12 端口约定：fs 9101 … exporter 9104，ss 9105）
DEFAULT_METRICS_PORT = 9105

# 冒烟内置样例（任务二：5–10 条真实风格事件，覆盖不同打分画像）
SMOKE_SAMPLES: tuple[str, ...] = (
    "我被公司裁员了，今天是我最后一天上班，心情特别低落。",
    "记住：我的过敏原是花生和尘螨，任何饮食建议都要避开。",
    "今天天气不错，随便聊了聊周末去哪玩。",
    "我们决定把 Q3 的发布日期从 8 月 15 日推迟到 9 月 1 日，之前说的 8 月 15 日作废。",
    "我下周三下午三点要和房东签续租合同，提醒我提前准备身份证复印件。",
    "我刚跑完人生第一个半程马拉松，成绩 2 小时 05 分！",
)


def _sample_event(content: str, n: int) -> ExStreamEvent:
    """冒烟/验证路径的一次性信封（space_id 仅形态占位，不触存储）。"""
    return ExStreamEvent(
        space_id="ss_offline",
        event_id=f"offline-{n}",
        n=n,
        content=content,
        agent_actor_id=None,
        account_id=None,
        tau_ms=None,
        ref_conflict=None,
        created_at_ms=int(time.time() * 1000),
    )


def _cmd_worker(args) -> int:
    config = SSConfig.from_env()  # LLM 三变量缺失 fail-closed
    client = pulsar_client()
    ex_cluster = ex_cassandra_cluster()
    control_cluster = cassandra_cluster()
    try:
        store = MappingTableControlPlaneStore(control_cluster.connect())
        store.ensure_tables()
        deps = worker.WorkerDeps(
            scorer=LLMScorer(config),
            ex_session=ex_cluster.connect(),
            publisher=worker.ResultPublisher(client),
            control_store=store,
            emit=worker.default_emit,
            config=config,
        )
        if args.once:
            worker.run_once(client, deps)
            return 0
        # M12：常驻形态接日志管线 + /metrics 暴露口（一次性 --once 不起）
        logschema_configure()
        start_metrics_server(metrics_port_from_env(DEFAULT_METRICS_PORT))
        worker.run_forever(client, deps)
        return 0
    finally:
        client.close()
        ex_cluster.shutdown()
        control_cluster.shutdown()


def _cmd_smoke(args) -> int:
    """任务二冒烟：真实 API 小批量，验证端点可达/模型可用/六维稳定解析。"""
    config = SSConfig.from_env()
    scorer = LLMScorer(config)
    failures = 0
    for i, content in enumerate(SMOKE_SAMPLES, start=1):
        t0 = time.perf_counter()
        try:
            result, usage = score_event(_sample_event(content, i), scorer=scorer, config=config)
        except ScoringError as exc:
            failures += 1
            print(f"[{i}] FAIL {exc}")
            continue
        latency_ms = int((time.perf_counter() - t0) * 1000)
        dims = " ".join(f"{d}={result.dims[d]:.2f}" for d in DIMENSIONS)
        flag = " degraded" if result.degraded else ""
        in_tok, out_tok = usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)
        print(
            f"[{i}] ok{flag} s={result.s:.3f} {dims} "
            f"({latency_ms}ms, {in_tok}+{out_tok}tok) :: {content[:24]}…"
        )
    print(f"smoke: {len(SMOKE_SAMPLES) - failures}/{len(SMOKE_SAMPLES)} 成功")
    return 1 if failures else 0


def _stats(values: list[float]) -> dict:
    ordered = sorted(values)
    return {
        "mean": round(statistics.fmean(ordered), 4),
        "std": round(statistics.pstdev(ordered), 4),
        "min": round(ordered[0], 4),
        "p50": round(statistics.median(ordered), 4),
        "max": round(ordered[-1], 4),
        # 塌缩检测：全样本该维近似恒同值（标准差近零）→ 维度无区分度
        "collapsed": bool(statistics.pstdev(ordered) < 0.05),
    }


def _cmd_validate(args) -> int:
    """任务三稳定性验证：真实样例集全量打分 → 分布/塌缩/成本报告（JSON）。"""
    config = SSConfig.from_env()
    scorer = LLMScorer(config)
    samples = [
        json.loads(line) for line in Path(args.samples).read_text().splitlines() if line.strip()
    ]
    dims_seen: dict[str, list[float]] = {d: [] for d in DIMENSIONS}
    s_values: list[float] = []
    latencies_ms: list[float] = []
    prompt_tokens = completion_tokens = 0
    failed = degraded = 0
    for i, sample in enumerate(samples, start=1):
        t0 = time.perf_counter()
        try:
            result, usage = score_event(
                _sample_event(sample["content"], i), scorer=scorer, config=config
            )
        except ScoringError as exc:
            failed += 1
            print(f"[{i}] FAIL {exc}", file=sys.stderr)
            continue
        latencies_ms.append((time.perf_counter() - t0) * 1000)
        prompt_tokens += usage.get("prompt_tokens", 0)
        completion_tokens += usage.get("completion_tokens", 0)
        degraded += int(result.degraded)
        for d in DIMENSIONS:
            dims_seen[d].append(result.dims[d])
        s_values.append(result.s)
    scored = len(s_values)
    if scored == 0:
        print("validate: 全部失败，无分布可统计", file=sys.stderr)
        return 1
    cost = None
    if config.price_input_per_1m or config.price_output_per_1m:
        cost = round(
            prompt_tokens / 1e6 * config.price_input_per_1m
            + completion_tokens / 1e6 * config.price_output_per_1m,
            6,
        )
    report = {
        "model": config.llm_model,
        "weights": config.weights,
        "sample_count": len(samples),
        "scored": scored,
        "failed": failed,
        "degraded": degraded,
        "dimensions": {d: _stats(v) for d, v in dims_seen.items()},
        "s": _stats(s_values),
        "tokens": {"prompt": prompt_tokens, "completion": completion_tokens},
        "estimated_cost_usd": cost,
        "latency_ms": {
            "mean": round(statistics.fmean(latencies_ms), 1),
            "p50": round(statistics.median(latencies_ms), 1),
            "max": round(max(latencies_ms), 1),
        },
        "per_event": {
            "tokens": round((prompt_tokens + completion_tokens) / scored, 1),
            "cost_usd": round(cost / scored, 8) if cost is not None else None,
            "latency_ms": round(statistics.fmean(latencies_ms), 1),
        },
    }
    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    collapsed = [d for d in DIMENSIONS if report["dimensions"][d]["collapsed"]]
    print(f"validate: {scored}/{len(samples)} 成功，degraded={degraded}，failed={failed}")
    print(f"塌缩维度: {collapsed or '无'}；s 分布 {report['s']}")
    print(f"报告已写入 {args.out}")
    return 0


@redline1_exempt(
    worker="ss-scorer",
    reason=(
        "消费 ex-events Pulsar 消息（信封自带 space_id、与 topic 名校验），无枚举无跨 "
        "space 查询；n 连续性补偿按 space 单 keyspace 区间读取；故障隔离 = 逐消息处理"
    ),
    cadence="Pulsar 推送节奏；空轮 receive timeout（默认 1s）+ run_forever 轮间 sleep 1s",
)
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lethefield_ss", description="SS 显著性打分服务（M14）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_worker = sub.add_parser("worker", help="打分 worker（ex-events consumer，常驻）")
    p_worker.add_argument("--once", action="store_true", help="单轮后排空退出（测试/巡检用）")

    sub.add_parser("smoke", help="真实 API 小批量冒烟（任务二：端点/模型/解析验证）")

    p_validate = sub.add_parser("validate", help="打分稳定性验证（任务三：分布/塌缩/成本报告）")
    p_validate.add_argument("--samples", required=True, help="样例集 JSONL（content 字段）")
    p_validate.add_argument("--out", required=True, help="报告输出路径（JSON）")

    args = parser.parse_args(argv)
    if args.command == "worker":
        return _cmd_worker(args)
    if args.command == "smoke":
        return _cmd_smoke(args)
    return _cmd_validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
