"""契约 5（训练管线销毁指令）单测：schema serde、space_ref 不透明哈希、版本 fail-closed。"""

import pytest
from lethefield_clients import SpaceDestroyCommand, control_topic, space_ref_of
from lethefield_clients.training_control import COMMAND_SPACE_DESTROY, CONTRACT_VERSION


def test_space_ref_is_opaque_hash_and_stable():
    ref = space_ref_of("sp1")
    assert ref == space_ref_of("sp1")  # 稳定（成组定位前提）
    assert ref != space_ref_of("sp2")
    assert "sp1" not in ref  # 不暴露 space_id 明文
    assert len(ref) == 64  # sha256 hex


def test_command_serde_roundtrip():
    cmd = SpaceDestroyCommand(space_ref=space_ref_of("sp1"), initiator="ops", ticket_ref="t-1")
    restored = SpaceDestroyCommand.from_json(cmd.to_json())
    assert restored.space_ref == cmd.space_ref
    assert restored.command_type == COMMAND_SPACE_DESTROY
    assert restored.initiator == "ops"
    assert restored.ticket_ref == "t-1"
    assert restored.timestamp == cmd.timestamp
    assert restored.v == CONTRACT_VERSION


def test_command_payload_contains_no_space_id():
    cmd = SpaceDestroyCommand(space_ref=space_ref_of("sp1"), initiator="ops", ticket_ref="t-1")
    assert '"sp1"' not in cmd.to_json()


def test_from_json_rejects_unknown_version():
    cmd = SpaceDestroyCommand(space_ref="x" * 64, initiator="ops", ticket_ref="t-1")
    bad = cmd.to_json().replace(f'"v": {CONTRACT_VERSION}', '"v": 999')
    with pytest.raises(ValueError, match="版本不符"):
        SpaceDestroyCommand.from_json(bad)


def test_from_json_rejects_unknown_command_type():
    bad = (
        '{"v": 1, "command_type": "nuke", "space_ref": "r", '
        '"initiator": "ops", "ticket_ref": "t", "timestamp": "2026-01-01T00:00:00+00:00"}'
    )
    with pytest.raises(ValueError, match="未知指令类型"):
        SpaceDestroyCommand.from_json(bad)


def test_control_topic_is_training_tenant():
    assert control_topic() == "persistent://lethefield-training/control/space-destroy"
