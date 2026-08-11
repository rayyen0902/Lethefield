"""结构化日志事件 schema（space 粒度明细的统一格式）。

设计依据：开发文档 M12 / 设计文档 §19.6——space 粒度明细走日志管线
（异步批量进运维日志 ES），本 schema 是离线标定指标的原料，
也是训练管线入料口 ③ 的接口。

注意：`space_id` 在本 schema 中是合法字段（明细事件必须可定位 space），
它只被禁止作为聚合指标标签（见 lethefield-metrics 的标签黑名单）。
"""

from lethefield_logschema.es_sink import EsLogShipper, configure, emit
from lethefield_logschema.events import LogEvent

__all__ = ["EsLogShipper", "LogEvent", "configure", "emit"]
