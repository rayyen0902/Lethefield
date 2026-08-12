"""stream_publisher 单测（M14）：有限重试 + 退避、耗尽抛 PublishError、producer 缓存。"""

import pytest
from lethefield_api.stream_publisher import (
    PUBLISH_MAX_ATTEMPTS,
    ExStreamPublisher,
    PublishError,
)
from lethefield_clients.ex_stream import ExStreamEvent


class FakeProducer:
    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.sent: list[bytes] = []
        self.closed = False

    def send(self, payload: bytes) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("broker 抖动（fake）")
        self.sent.append(payload)

    def close(self) -> None:
        self.closed = True


class FakeClient:
    def __init__(self, producer: FakeProducer) -> None:
        self.producer = producer
        self.topics: list[str] = []

    def create_producer(self, topic: str) -> FakeProducer:
        self.topics.append(topic)
        return self.producer


def _event(space_id: str = "demo") -> ExStreamEvent:
    return ExStreamEvent(
        space_id=space_id,
        event_id="e1",
        n=1,
        content="c",
        agent_actor_id="a",
        account_id="acc",
        tau_ms=None,
        ref_conflict=None,
        created_at_ms=1,
    )


def test_publish_success_sends_envelope():
    producer = FakeProducer()
    publisher = ExStreamPublisher(FakeClient(producer))
    publisher.publish(_event())
    assert len(producer.sent) == 1
    assert ExStreamEvent.from_json(producer.sent[0].decode()).event_id == "e1"


def test_publish_retries_then_succeeds():
    producer = FakeProducer(fail_times=1)
    publisher = ExStreamPublisher(FakeClient(producer))
    publisher.publish(_event())  # 第一次失败，重试成功
    assert len(producer.sent) == 1


def test_publish_exhausted_raises_publish_error():
    producer = FakeProducer(fail_times=PUBLISH_MAX_ATTEMPTS + 1)
    publisher = ExStreamPublisher(FakeClient(producer))
    with pytest.raises(PublishError, match="重试耗尽"):
        publisher.publish(_event())
    assert producer.sent == []


def test_producer_cached_per_space():
    producer = FakeProducer()
    client = FakeClient(producer)
    publisher = ExStreamPublisher(client)
    publisher.publish(_event("sp_a"))
    publisher.publish(_event("sp_a"))
    publisher.publish(_event("sp_b"))
    assert client.topics == [
        "persistent://lethefield/sp_a/ex-events",
        "persistent://lethefield/sp_b/ex-events",
    ]  # 每 space 只建一次 producer


def test_close_closes_all_producers():
    producer = FakeProducer()
    publisher = ExStreamPublisher(FakeClient(producer))
    publisher.publish(_event())
    publisher.close()
    assert producer.closed is True
