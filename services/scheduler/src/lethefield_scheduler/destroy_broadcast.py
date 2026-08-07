"""训练管线销毁广播生产者（M10，契约 5 硬约束 1 的实现点）。

**生产者等 broker ack**：pulsar-client 的 send 同步阻塞到 broker 确认才返回，
天然满足"不是 fire-and-forget"。有限重试后仍失败抛 BroadcastError——
调用方（destroy.py 第 4 步）据此标记失败 + 告警 + 留痕，禁止静默进入第 5 步。
"""

import logging
import time
from collections.abc import Callable

from lethefield_clients import SpaceDestroyCommand, control_topic, space_ref_of
from pulsar import Client

logger = logging.getLogger(__name__)


class BroadcastError(RuntimeError):
    """销毁广播最终失败（已重试耗尽）——注销流水线第 4 步必须因此中止。"""


def make_broadcast(
    pulsar: Client,
    *,
    initiator: str = "lethefield-scheduler",
    max_retries: int = 3,
    retry_interval_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[str], None]:
    """构造 destroy 流水线的 broadcast_destroy 回调（space_id → 发契约 5 指令）。

    返回闭包签名对齐 destroy_space 的 broadcast_destroy 注入点；失败抛 BroadcastError。
    """
    try:
        producer = pulsar.create_producer(control_topic())
    except Exception as exc:  # noqa: BLE001 — 建生产者失败 = 广播通道不可用
        raise BroadcastError(f"销毁广播建生产者失败：{exc}") from exc

    def broadcast(space_id: str) -> None:
        command = SpaceDestroyCommand(
            space_ref=space_ref_of(space_id),
            initiator=initiator,
            # 注销工单引用：1.0 无工单系统（M17 运维操作面），以 space_ref+时间戳
            # 生成可回溯引用；M17 落地后换真实工单号（契约 5 字段不变，值语义升级）
            ticket_ref=f"destroy:{command_ref(space_id)}",
        )
        payload = command.to_json().encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                producer.send(payload)  # 同步等 broker ack
                logger.info(
                    "space.destroy.broadcast.sent space_ref=%s ticket_ref=%s",
                    command.space_ref,
                    command.ticket_ref,
                )
                return
            except Exception as exc:  # noqa: BLE001 — 重试边界收一切 broker 侧异常
                last_exc = exc
                logger.warning(
                    "space.destroy.broadcast.retry attempt=%d/%d space_ref=%s err=%s",
                    attempt,
                    max_retries,
                    command.space_ref,
                    exc,
                )
                if attempt < max_retries:
                    sleep(retry_interval_seconds)
        raise BroadcastError(
            f"销毁广播失败（已重试 {max_retries} 次，broker 未确认）：{last_exc}"
        ) from last_exc

    return broadcast


def command_ref(space_id: str) -> str:
    """工单引用占位组件：space_ref 前 12 位 + 发出时刻（同一 space 重试可区分）。"""
    return f"{space_ref_of(space_id)[:12]}:{int(time.time())}"
