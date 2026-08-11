"""指标 registry 封装：命名规则与标签白/黑名单的代码层强制。

设计依据：开发文档 M12 / 设计文档 §19.5——
- 命名规则 `lethefield_<域>_<名称>_<单位>`（Prometheus 标准单位）
- 标签白名单：service/instance/cell_id/tier/枚举类
- 标签黑名单：space_id、node_key（防基数爆炸 + 守红线 1）
"""

from lethefield_metrics.exposition import metrics_port_from_env, start_metrics_server
from lethefield_metrics.registry import (
    LABEL_BLACKLIST,
    LABEL_WHITELIST,
    UNIT_SUFFIXES,
    counter,
    gauge,
    histogram,
)

__all__ = [
    "LABEL_BLACKLIST",
    "LABEL_WHITELIST",
    "UNIT_SUFFIXES",
    "counter",
    "gauge",
    "histogram",
    "metrics_port_from_env",
    "start_metrics_server",
]
