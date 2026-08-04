"""结构化日志事件模型与 JSONL 序列化。"""

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LogEvent(BaseModel):
    """space 粒度明细日志事件。

    - `space_id` 可为 None：仅用于真正的服务级事件（如启动、配置加载）。
      凡是与某 space 数据相关的事件必须携带 space_id（红线 1 的可审计性前提）。
    - `payload` 为事件特定内容，由各服务自定义结构，本 schema 不约束。
    """

    model_config = ConfigDict(extra="forbid")

    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    service: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    space_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_jsonl(self) -> str:
        """序列化为单行 JSON（JSONL）。"""
        return self.model_dump_json()

    @classmethod
    def from_jsonl(cls, line: str) -> "LogEvent":
        """从单行 JSON 反序列化，格式不符抛 ValidationError。"""
        return cls.model_validate_json(line)
