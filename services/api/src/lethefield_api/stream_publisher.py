"""EX→Pulsar 生产侧（M14，v1.2 修订记录第 20 条定案）——显式登记的 producer 依赖单点。

定案：经验事件由 API 摄入路径在 `append_experience` 落库确认后发布到
`persistent://lethefield/{space_id}/ex-events`；有限重试 + 退避；**发布失败不阻塞
同步返回**（EX 已是 SoT，失败由调用方告警 + 指标，消费侧 n 连续性校验兜底自愈）。
M5"API 不依赖 Pulsar"红线已修订为"**同步返回路径**不依赖 Pulsar"——Pulsar import
只允许出现在本模块（集成测试结构性断言按此口径）。

信封与 topic 命名单点在 `lethefield_clients.ex_stream`。
"""

import time

from lethefield_clients.ex_stream import ExStreamEvent, ex_events_topic

# 发布有限重试（定案：有限重试 + 退避；耗尽抛 PublishError 由调用方告警，不重试到死）
PUBLISH_MAX_ATTEMPTS = 3
PUBLISH_BACKOFF_SECONDS = 0.2


class PublishError(Exception):
    """发布重试耗尽后的最终失败（调用方告警，不阻塞同步返回路径）。"""


class ExStreamPublisher:
    """ex-events 发布器：per-space producer 懒建缓存，同步等 broker ack。"""

    def __init__(self, client) -> None:
        self._client = client
        self._producers: dict[str, object] = {}

    def _producer(self, space_id: str):
        producer = self._producers.get(space_id)
        if producer is None:
            producer = self._client.create_producer(ex_events_topic(space_id))
            self._producers[space_id] = producer
        return producer

    def publish(self, event: ExStreamEvent) -> None:
        """发布一条经验事件信封；重试耗尽抛 PublishError。"""
        payload = event.to_json().encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(PUBLISH_MAX_ATTEMPTS):
            try:
                self._producer(event.space_id).send(payload)
                return
            except Exception as exc:  # broker 抖动/连接断裂：有限重试 + 退避
                last_exc = exc
                if attempt + 1 < PUBLISH_MAX_ATTEMPTS:
                    time.sleep(PUBLISH_BACKOFF_SECONDS)
        raise PublishError(
            f"ex-events 发布失败（{PUBLISH_MAX_ATTEMPTS} 次重试耗尽）："
            f"space={event.space_id} event_id={event.event_id}"
        ) from last_exc

    def close(self) -> None:
        for producer in self._producers.values():
            producer.close()
        self._producers.clear()
