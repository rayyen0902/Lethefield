"""训练管线销毁指令最小接收 sink 单测：接收记录（决策留痕）+ 畸形指令不静默。"""

import pytest
from lethefield_clients import SpaceDestroyCommand, space_ref_of
from lethefield_scheduler.training_control_sink import handle_message


def test_handle_message_emits_receipt_record():
    cmd = SpaceDestroyCommand(space_ref=space_ref_of("sp1"), initiator="ops", ticket_ref="t-1")
    emitted = []
    handle_message(cmd.to_json().encode(), emit=emitted.append)
    assert len(emitted) == 1
    event = emitted[0]
    assert event.event_type == "space_destroy_received"
    assert event.payload["space_ref"] == cmd.space_ref
    assert event.payload["ticket_ref"] == "t-1"
    assert event.payload["initiator"] == "ops"
    assert '"sp1"' not in event.to_jsonl()  # 不明文暴露 space_id


def test_handle_message_rejects_malformed():
    with pytest.raises(ValueError):
        handle_message(b'{"v": 999}', emit=lambda e: None)


def test_handle_message_rejects_non_json():
    with pytest.raises(Exception, match=""):  # noqa: B017 — json 解析异常类型由实现定
        handle_message(b"not-json", emit=lambda e: None)
