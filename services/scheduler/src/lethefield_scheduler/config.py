"""调度器配置（阈值/容量上限为初值，设计文档 §17.6 待容量标定后调整）。"""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SchedulerConfig:
    # 水位阈值（§17.3 初值）：全维 <filling → open；任一维 [filling, closed) → filling；
    # 任一维 ≥closed → closed（只出不进、告警）
    filling_threshold: float = 0.7
    closed_threshold: float = 0.9
    # 容量上限（水位 = 实测/上限）：Cassandra 单集群 keyspace 约 1000~2000（§15.3），
    # ES 分片堆开销更紧——均为业界经验值初值，待 Cell 容量标定
    keyspace_cap: int = 1500
    es_shard_cap: int = 3000
    # Pulsar：全局集群池（§18.2），每 space 一个 namespace，tenant 固定
    pulsar_tenant: str = "lethefield"
    pulsar_admin_url: str = field(
        default_factory=lambda: os.environ.get(
            "LETHEFIELD_PULSAR_ADMIN_URL", "http://localhost:8080"
        )
    )
    # 单节点起步部署的集群标识（映射表登记字段；多集群形态随 M10/生产化扩展）
    ex_cluster_id: str = "ex-local"
    pulsar_cluster_id: str = "pulsar-local"


DEFAULT_CONFIG = SchedulerConfig()

# schema 变更在套件负载下可超默认 10s 客户端超时（M9 CI 实测）——开通/注销 DDL 统一放宽
DDL_TIMEOUT_SECONDS = 60.0
