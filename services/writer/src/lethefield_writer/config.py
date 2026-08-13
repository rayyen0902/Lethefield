"""M15 运行配置（env 可配占位，禁硬编码；可靠性参数进 §20 待标定）。

Embedding 四变量（LETHEFIELD_EMBED_BASE_URL / API_KEY / MODEL / DIMS）从仓库根
`.env` 读取（dotenv 单点加载在本模块）；缺失 fail-closed——不静默降级（与 M14
SS LLM 三变量同款纪律，v1.2 修订记录第 21 条）。key 纪律：只进 Authorization
header——不进日志、不进指标标签、不进异常消息体。

一致性规则（修订记录第 23 条④）：共享 rms_vectors 索引内向量必须同模型，
embed_dims 必须与索引 mapping dims 一致（worker 启动时 ensure_vectors_index
校验，不符拒启动）；embedding 模型变更 = 向量全量重建，选型与变更进决策留痕。
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# .env 单点加载（幂等；找不到文件不报错——env 也可由进程环境直接注入）
load_dotenv()


@dataclass(frozen=True)
class WriterConfig:
    # embedding 端点（OpenAI 兼容 /embeddings；空值仅允许于不触嵌入的路径，
    # from_env 强制非空）
    embed_base_url: str = ""
    embed_api_key: str = ""
    embed_model: str = ""
    embed_dims: int = 0  # 必须等于 rms_vectors mapping dims（启动校验 fail-closed）
    # embedding 调用可靠性
    embed_timeout_seconds: float = 30.0
    embed_max_retries: int = 2  # 超时/5xx/连接错误的有限重试次数
    embed_retry_backoff_seconds: float = 1.0
    # consumer：应用层死信前的最大重投次数（nack 重投耗尽后原文转 DLQ topic）
    max_redeliver_count: int = 3
    # nack 后的重投延迟（毫秒；测试注入小值加速 DLQ 路径）
    nack_redelivery_delay_ms: int = 1000
    # space 列表（consumer 集合）刷新节流（秒；逐 space 订阅，枚举走 ControlPlaneStore）
    topic_discovery_seconds: int = 5
    receive_timeout_ms: int = 1000

    @classmethod
    def from_env(cls, *, require_embed: bool = True) -> "WriterConfig":
        """从 env 构造（.env 已在本模块单点加载）。require_embed 时四变量缺失 fail-closed。"""
        base_url = os.environ.get("LETHEFIELD_EMBED_BASE_URL", "")
        api_key = os.environ.get("LETHEFIELD_EMBED_API_KEY", "")
        model = os.environ.get("LETHEFIELD_EMBED_MODEL", "")
        dims = int(os.environ.get("LETHEFIELD_EMBED_DIMS", "0"))
        if require_embed and not all((base_url, api_key, model, dims > 0)):
            raise ValueError(
                "embedding 配置不全：LETHEFIELD_EMBED_BASE_URL / LETHEFIELD_EMBED_API_KEY / "
                "LETHEFIELD_EMBED_MODEL / LETHEFIELD_EMBED_DIMS 需在 .env 或进程环境中"
                "齐备（fail-closed，不静默降级）"
            )
        return cls(
            embed_base_url=base_url,
            embed_api_key=api_key,
            embed_model=model,
            embed_dims=dims,
            embed_timeout_seconds=float(
                os.environ.get("LETHEFIELD_EMBED_TIMEOUT_SECONDS", cls.embed_timeout_seconds)
            ),
            embed_max_retries=int(
                os.environ.get("LETHEFIELD_EMBED_MAX_RETRIES", cls.embed_max_retries)
            ),
            max_redeliver_count=int(
                os.environ.get("LETHEFIELD_WRITER_MAX_REDELIVER", cls.max_redeliver_count)
            ),
            nack_redelivery_delay_ms=int(
                os.environ.get(
                    "LETHEFIELD_WRITER_NACK_REDELIVERY_DELAY_MS", cls.nack_redelivery_delay_ms
                )
            ),
            receive_timeout_ms=int(
                os.environ.get("LETHEFIELD_WRITER_RECEIVE_TIMEOUT_MS", cls.receive_timeout_ms)
            ),
        )


DEFAULT_CONFIG = WriterConfig()
