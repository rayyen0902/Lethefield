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
    # 业务 namespace 策略初值（v0.7 namespace 级配额/策略隔离落地，§20 待标定）：
    # 消息是传输管道（消费后价值即转移到 EX），retention 不需长；backlog 配额
    # producer_request_hold——背压对生产者明确可见，不静默丢弃（DMS 同哲学）。
    # 约束：backlog 配额必须 < retention size（Pulsar 412 拒绝，M10 实测）
    pulsar_namespace_retention_minutes: int = 10_080  # 7 天
    pulsar_namespace_retention_size_mb: int = 2048
    pulsar_namespace_backlog_quota_mb: int = 512
    # 训练控制 namespace retention（契约 5：持久化 topic 本身是指令留存证据，
    # 与业务流独立；审计级保留初值 90 天，待合规标定）
    training_control_retention_minutes: int = 129_600  # 90 天
    # 销毁广播生产者重试（等 broker ack，最终失败 → 注销第 4 步失败中止，见 destroy.py）
    broadcast_max_retries: int = 3
    # 单节点起步部署的集群标识（映射表登记字段；多集群形态随 M10/生产化扩展）
    ex_cluster_id: str = "ex-local"
    pulsar_cluster_id: str = "pulsar-local"


DEFAULT_CONFIG = SchedulerConfig()

# schema 变更在套件负载下可超默认 10s 客户端超时（M9 CI 实测）——开通/注销 DDL 统一放宽
DDL_TIMEOUT_SECONDS = 60.0


def cell_host_endpoints(cell_id: str) -> dict[str, str | None]:
    """本地形态的 cell → host 侧连接参数（M10 迁移/演练构造目标端客户端用）。

    值全 None = factories env 默认（cell-local）；cell-local-2 对应 compose
    `cell2` profile 端口（按需起，heap 上限见 docker-compose 注释），env 可覆盖。
    生产多 Cell 的连接路由（按 cell_id 分连接池）属生产化课题（M9 遗留）。
    """
    if cell_id == "cell-local-2":
        return {
            "gremlin_url": os.environ.get(
                "LETHEFIELD_CELL2_GREMLIN_URL", "ws://localhost:8183/gremlin"
            ),
            "cassandra_port": os.environ.get("LETHEFIELD_CELL2_CASSANDRA_PORT", "9142"),
            "es_url": os.environ.get("LETHEFIELD_CELL2_ES_URL", "http://localhost:9300"),
        }
    return {"gremlin_url": None, "cassandra_port": None, "es_url": None}
