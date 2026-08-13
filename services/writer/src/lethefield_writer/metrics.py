"""M15 writer 指标单点（命名/标签规则由 libs/metrics 工厂强制；显式 REGISTRY 注册）。"""

from lethefield_metrics import counter
from prometheus_client import REGISTRY

# worker /metrics 暴露口默认端口（M12 端口约定：fs 9101 … ss 9105，writer 9106）
DEFAULT_METRICS_PORT = 9106

NODE_WRITE_TOTAL = counter(
    "lethefield_writer_node_write_total",
    "写入链建点计数（created=新建或部分补全 / duplicate=重复投递零写入 / "
    "compensated=n 缺口补偿建点）",
    ["result"],
    registry=REGISTRY,
)
N_GAP_TOTAL = counter(
    "lethefield_writer_n_gap_total",
    "scoring-results 消费侧 n 连续性缺口计数（发布失败的自愈触发点）",
    registry=REGISTRY,
)
DLQ_TOTAL = counter(
    "lethefield_writer_dlq_total",
    "重试耗尽转死信的打分结果计数",
    registry=REGISTRY,
)
EMBED_CALLS_TOTAL = counter(
    "lethefield_writer_embed_calls_total",
    "embedding 调用计数（标定线：调用量）",
    ["result"],
    registry=REGISTRY,
)
EMBED_TOKENS_TOTAL = counter(
    "lethefield_writer_embed_tokens_total",
    "embedding token 用量（标定线：成本曲线输入）",
    ["type"],
    registry=REGISTRY,
)
