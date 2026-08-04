"""CLI：python -m lethefield_clock_monitor [--threshold 秒]

巡检出口：打印各组件偏移，任一超阈值时输出结构化告警日志事件并以退出码 1 告警。
"""

import argparse
import sys

from lethefield_logschema import LogEvent

from lethefield_clock_monitor import check_offsets, collect_all
from lethefield_clock_monitor.check import DEFAULT_THRESHOLD_SECONDS


def main() -> int:
    parser = argparse.ArgumentParser(prog="clock_monitor", description="时钟偏移监控（红线 6）")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD_SECONDS)
    args = parser.parse_args()

    samples = collect_all()
    for s in samples:
        offset = f"{s.offset_seconds:+.3f}s" if s.offset_seconds != float("inf") else "采集失败"
        print(f"{s.component:<16} offset={offset}")

    alerts = check_offsets(samples, args.threshold)
    for alert in alerts:
        # 告警以结构化日志事件输出（告警通道选型属 M17 决策留痕项，此为事件源）
        print(
            LogEvent(
                service="clock-monitor",
                event_type="clock_offset_alert",
                payload={"alert": alert, "threshold_seconds": args.threshold},
            ).to_jsonl(),
            file=sys.stderr,
        )

    if alerts:
        return 1
    print(f"红线 6 巡检通过：全部组件偏移在 ±{args.threshold}s 内")
    return 0


if __name__ == "__main__":
    sys.exit(main())
