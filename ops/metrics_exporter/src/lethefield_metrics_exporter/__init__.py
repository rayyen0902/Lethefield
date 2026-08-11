"""离线聚合指标 worker（M12，开发文档 §13 / 设计文档 §19）。

红线 1 边界（静态测试强制）：只读 es-ops 日志管线、Cassandra 系统表
（system.size_estimates）、控制面映射表、Redis 状态键——**禁止 import 任何
业务存储读层**（gremlin/ex_n/elasticsearch 业务索引直查在禁止清单外的只有
rms_vectors 的 _stats/_count 元数据口径）。
"""

from lethefield_metrics_exporter.exporter import ExporterDeps, run_once

__all__ = ["ExporterDeps", "run_once"]
