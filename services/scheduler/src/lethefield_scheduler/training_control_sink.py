"""训练管线销毁指令最小接收 consumer（M10，契约 5 硬约束：链路真实可达的证明）。

订阅训练 tenant 控制 topic，收到契约 5 销毁指令 → 落接收记录（决策留痕事件，
含 space_ref/ticket_ref）→ ack。subscription 用 durable 命名——consumer 离线
期间指令在 broker 侧积压不丢（backlog 由 ops/ingest_dms 监控告警）。

**M11 替换路径**：真实加工 worker 消费同一 topic（建议复用同一 subscription 名
以继承积压），接口零变更；本 sink 同时是 M11 worker 的协议参照实现。

CLI：python -m lethefield_scheduler.training_control_sink [--once]
"""

import argparse
import sys
from collections.abc import Callable

import pulsar
from lethefield_clients import (
    DESTROY_SUBSCRIPTION,
    SpaceDestroyCommand,
    control_topic,
    pulsar_client,
)
from lethefield_logschema import LogEvent
from pulsar import Client


def handle_message(data: bytes, *, emit: Callable[[LogEvent], None]) -> None:
    """处理一条契约 5 指令：校验 schema → 落接收记录。schema 不符抛 ValueError
    （不 ack，留 broker 重投——畸形指令不允许静默丢弃）。"""
    command = SpaceDestroyCommand.from_json(data.decode("utf-8"))
    emit(
        LogEvent(
            service="training-control-sink",
            event_type="space_destroy_received",
            payload={
                "space_ref": command.space_ref,
                "command_type": command.command_type,
                "initiator": command.initiator,
                "ticket_ref": command.ticket_ref,
                "command_timestamp": command.timestamp.isoformat(),
            },
        )
    )


def run_once(client: Client, *, emit: Callable[[LogEvent], None], timeout_ms: int = 3000) -> int:
    """单轮：排空当前可达消息并逐条处理（返回处理条数；测试/巡检用）。"""
    consumer = client.subscribe(control_topic(), DESTROY_SUBSCRIPTION)
    processed = 0
    try:
        while True:
            try:
                msg = consumer.receive(timeout_millis=timeout_ms)
            except pulsar.Timeout:  # 超时即当前无更多消息（其他异常不静默上抛）
                break
            handle_message(msg.data(), emit=emit)
            consumer.acknowledge(msg)
            processed += 1
    finally:
        consumer.close()
    return processed


def run_forever(client: Client, *, emit: Callable[[LogEvent], None]) -> None:
    """常驻消费循环（生产形态；M11 worker 接管后本 sink 退出）。"""
    consumer = client.subscribe(control_topic(), DESTROY_SUBSCRIPTION)
    try:
        while True:
            msg = consumer.receive()
            handle_message(msg.data(), emit=emit)
            consumer.acknowledge(msg)
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="training_control_sink", description="训练管线销毁指令最小接收 consumer（M10）"
    )
    parser.add_argument("--once", action="store_true", help="单轮排空后退出（测试/巡检用）")
    args = parser.parse_args(argv)

    emit = lambda event: print(event.to_jsonl(), file=sys.stderr)  # noqa: E731
    client = pulsar_client()
    try:
        if args.once:
            processed = run_once(client, emit=emit)
            print(f"[ok] 本轮处理 {processed} 条销毁指令")
        else:
            run_forever(client, emit=emit)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
