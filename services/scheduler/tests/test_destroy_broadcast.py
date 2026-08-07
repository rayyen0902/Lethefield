"""销毁广播生产者单测（fake Pulsar）：等 ack、有限重试、最终失败抛 BroadcastError。"""

import pytest
from lethefield_clients import SpaceDestroyCommand, space_ref_of
from lethefield_scheduler.destroy_broadcast import BroadcastError, make_broadcast


class _FakeProducer:
    def __init__(self, failures_before_ok: int = 0, always_fail: bool = False) -> None:
        self.sent: list[bytes] = []
        self._failures_left = failures_before_ok
        self._always_fail = always_fail

    def send(self, payload: bytes) -> None:
        if self._always_fail or self._failures_left > 0:
            self._failures_left -= 1
            raise RuntimeError("broker 不可达")
        self.sent.append(payload)


class _FakePulsar:
    def __init__(self, producer: _FakeProducer) -> None:
        self._producer = producer
        self.topics: list[str] = []

    def create_producer(self, topic: str) -> _FakeProducer:
        self.topics.append(topic)
        return self._producer


def test_broadcast_sends_contract5_and_waits_ack():
    producer = _FakeProducer()
    broadcast = make_broadcast(_FakePulsar(producer))
    broadcast("sp1")
    assert len(producer.sent) == 1
    cmd = SpaceDestroyCommand.from_json(producer.sent[0].decode())
    assert cmd.space_ref == space_ref_of("sp1")
    assert cmd.initiator == "lethefield-scheduler"
    assert cmd.ticket_ref.startswith("destroy:")


def test_broadcast_uses_training_control_topic():
    pulsar = _FakePulsar(_FakeProducer())
    make_broadcast(pulsar)("sp1")
    assert pulsar.topics == ["persistent://lethefield-training/control/space-destroy"]


def test_broadcast_retries_then_succeeds():
    producer = _FakeProducer(failures_before_ok=2)
    broadcast = make_broadcast(_FakePulsar(producer), max_retries=3, sleep=lambda s: None)
    broadcast("sp1")
    assert len(producer.sent) == 1  # 第三次成功


def test_broadcast_exhausted_raises():
    producer = _FakeProducer(always_fail=True)
    broadcast = make_broadcast(_FakePulsar(producer), max_retries=3, sleep=lambda s: None)
    with pytest.raises(BroadcastError, match="重试 3 次"):
        broadcast("sp1")
    assert producer.sent == []  # 未确认的指令不得算发出
