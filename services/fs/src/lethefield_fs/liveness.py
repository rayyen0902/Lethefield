"""Dead Man's Switch 巡检（M6）：sweep 停摆 = 忽视惩罚静默失效。

与 EX 摄入层"事件悄悄不再上传"是同构故障（设计文档 §7.5.1），因此 sweep worker
必须纳入同款式监控：worker 每轮写 Redis 心跳（见 worker.py），本巡检只读心跳判定
存活性，超窗口退出码 1 告警（clock_monitor 同款形态）。

用法：uv run python -m lethefield_fs.liveness [--stale-after 秒]
"""

import argparse
import time

from lethefield_clients import redis_client

from lethefield_fs.config import DEFAULT_SWEEP_CONFIG, HEARTBEAT_KEY


def check_liveness(
    redis,
    *,
    stale_after_seconds: float = DEFAULT_SWEEP_CONFIG.stale_after_seconds,
    now: float | None = None,
) -> list[str]:
    """存活性判定：返回告警消息列表（空 = 存活）。now 可注入（测试伪造时钟）。"""
    now = time.time() if now is None else now
    ts = redis.get(HEARTBEAT_KEY)
    if ts is None:
        return ["FS sweep 无心跳记录：worker 从未成功运行或心跳键丢失（忽视惩罚静默失效）"]
    lag = now - float(ts)
    if lag > stale_after_seconds:
        return [
            f"FS sweep 心跳停滞：{lag:.0f}s 无成功轮次，"
            f"超过告警窗口 {stale_after_seconds:.0f}s（忽视惩罚静默失效）"
        ]
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FS sweep 存活性巡检（Dead Man's Switch，M6）")
    parser.add_argument(
        "--stale-after",
        type=float,
        default=DEFAULT_SWEEP_CONFIG.stale_after_seconds,
        help="告警窗口（秒）：超过此时长无心跳即告警",
    )
    args = parser.parse_args(argv)

    alerts = check_liveness(redis_client(), stale_after_seconds=args.stale_after)
    for alert in alerts:
        print(f"[alert] {alert}")
    if not alerts:
        print("[ok] FS sweep 心跳正常")
    return 1 if alerts else 0


if __name__ == "__main__":
    raise SystemExit(main())
