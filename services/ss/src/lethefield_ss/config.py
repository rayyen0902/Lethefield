"""M14 运行配置（env 可配占位，禁硬编码；权重/降级参数进 §20 待标定）。

LLM 三变量（SS_LLM_BASE_URL / SS_LLM_API_KEY / SS_LLM_MODEL）从仓库根 `.env`
读取（dotenv 单点加载在本模块）；缺失 fail-closed——不静默降级（v1.2 修订记录
第 21 条定案）。key 纪律：不进日志、不进指标标签、不进异常消息体、不进测试快照。
"""

import json
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv
from lethefield_clients.ex_stream import DIMENSIONS

# 降级策略枚举（v1.2 修订记录第 22 条定案）
DEGRADE_NEUTRAL_MARK = "neutral_mark"  # 缺 1 维置中性值 + degraded 标记（默认）
DEGRADE_RETRY = "retry"  # 缺维即整单重试 → DLQ
DEGRADE_POLICIES: frozenset[str] = frozenset({DEGRADE_NEUTRAL_MARK, DEGRADE_RETRY})

# .env 单点加载（幂等；找不到文件不报错——env 也可由进程环境直接注入）
load_dotenv()


def _default_weights() -> dict[str, float]:
    """六维均权占位（待种子期真实数据标定；原始六维单独存储，调权无需重打分）。"""
    return {d: 1.0 / len(DIMENSIONS) for d in DIMENSIONS}


@dataclass(frozen=True)
class SSConfig:
    # LLM 端点（OpenAI 兼容；空值仅允许于不触 LLM 的路径，from_env 强制非空）
    llm_base_url: str = ""
    llm_api_key: str = ""
    llm_model: str = ""
    # 六维合成 s 权重（禁硬编码定案：配置占位，标定后只调合成逻辑）
    weights: dict[str, float] = field(default_factory=_default_weights)
    # 降级规则（显式定义，不允许代码隐式兜底）
    degrade_policy: str = DEGRADE_NEUTRAL_MARK
    degrade_neutral: float = 0.5  # 缺失维中性值（SS_DEGRADE_NEUTRAL 可配）
    # LLM 调用可靠性
    llm_timeout_seconds: float = 30.0
    llm_max_retries: int = 2  # 超时/5xx/连接错误的有限重试次数
    llm_retry_backoff_seconds: float = 1.0
    # consumer：应用层死信前的最大重投次数（nack 重投耗尽后原文转 DLQ topic）
    max_redeliver_count: int = 3
    # nack 后的重投延迟（毫秒；测试注入小值加速 DLQ 路径）
    nack_redelivery_delay_ms: int = 1000
    # space 列表（consumer 集合）刷新节流（秒；逐 space 订阅，枚举走 ControlPlaneStore）
    topic_discovery_seconds: int = 5
    receive_timeout_ms: int = 1000
    # 成本标定输入（$ / 1M tokens；0 = 未配置，报告只出 token 量不出成本）
    price_input_per_1m: float = 0.0
    price_output_per_1m: float = 0.0

    def __post_init__(self) -> None:
        if set(self.weights) != set(DIMENSIONS):
            raise ValueError(f"权重键不符：{sorted(self.weights)!r}（期望 {sorted(DIMENSIONS)}）")
        if any(w < 0 for w in self.weights.values()):
            raise ValueError(f"权重必须非负：{self.weights!r}")
        if self.degrade_policy not in DEGRADE_POLICIES:
            raise ValueError(
                f"未知降级策略 {self.degrade_policy!r}（枚举 {sorted(DEGRADE_POLICIES)}）"
            )
        if not 0.0 <= self.degrade_neutral <= 1.0:
            raise ValueError(f"degrade_neutral 越界 [0,1]：{self.degrade_neutral!r}")

    @classmethod
    def from_env(cls, *, require_llm: bool = True) -> "SSConfig":
        """从 env 构造（.env 已在本模块单点加载）。require_llm 时三变量缺失 fail-closed。"""
        base_url = os.environ.get("SS_LLM_BASE_URL", "")
        api_key = os.environ.get("SS_LLM_API_KEY", "")
        model = os.environ.get("SS_LLM_MODEL", "")
        if require_llm and not all((base_url, api_key, model)):
            raise ValueError(
                "LLM 配置不全：SS_LLM_BASE_URL / SS_LLM_API_KEY / SS_LLM_MODEL 需在 "
                ".env 或进程环境中齐备（fail-closed，不静默降级）"
            )
        weights_json = os.environ.get("LETHEFIELD_SS_WEIGHTS_JSON")
        weights = json.loads(weights_json) if weights_json else _default_weights()
        return cls(
            llm_base_url=base_url,
            llm_api_key=api_key,
            llm_model=model,
            weights={k: float(v) for k, v in weights.items()},
            degrade_policy=os.environ.get("SS_DEGRADE_POLICY", cls.degrade_policy),
            degrade_neutral=float(os.environ.get("SS_DEGRADE_NEUTRAL", cls.degrade_neutral)),
            llm_timeout_seconds=float(
                os.environ.get("LETHEFIELD_SS_LLM_TIMEOUT_SECONDS", cls.llm_timeout_seconds)
            ),
            llm_max_retries=int(
                os.environ.get("LETHEFIELD_SS_LLM_MAX_RETRIES", cls.llm_max_retries)
            ),
            max_redeliver_count=int(
                os.environ.get("LETHEFIELD_SS_MAX_REDELIVER", cls.max_redeliver_count)
            ),
            nack_redelivery_delay_ms=int(
                os.environ.get(
                    "LETHEFIELD_SS_NACK_REDELIVERY_DELAY_MS", cls.nack_redelivery_delay_ms
                )
            ),
            receive_timeout_ms=int(
                os.environ.get("LETHEFIELD_SS_RECEIVE_TIMEOUT_MS", cls.receive_timeout_ms)
            ),
            price_input_per_1m=float(
                os.environ.get("LETHEFIELD_SS_PRICE_INPUT_PER_1M", cls.price_input_per_1m)
            ),
            price_output_per_1m=float(
                os.environ.get("LETHEFIELD_SS_PRICE_OUTPUT_PER_1M", cls.price_output_per_1m)
            ),
        )


DEFAULT_CONFIG = SSConfig()
