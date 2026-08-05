"""FS sweep 配置（M6）。

FF 语义占位（N_neglect、grace_n、θ_base）一律从 lethefield_rms.ff.FFConfig 取，
本模块不复制——FFConfig 是 agent-level constants 的单点。
数值全为占位（正式标定属种子期，走参数标定流程）。
"""

from dataclasses import dataclass

# Dead Man's Switch 心跳键：全局键 + 每 space 键 f"{HEARTBEAT_KEY}:{space_id}"
# （Redis 键含 space_id 与 ex:n:{space} 同款，不属于聚合指标标签，不违反埋点规范）
HEARTBEAT_KEY = "fs:sweep:last_ok"


@dataclass(frozen=True)
class SweepConfig:
    """sweep worker 运行参数（占位值，标定流程调整）。"""

    sweep_interval_seconds: float = 60.0  # 循环节奏：只需显著快于 N_neglect 对应的事件推进速度
    consolidate_reinforce_threshold: int = 3  # 固化阈值：reinforce_count 达此值且期间无 conflict
    near_horizon_margin: int = 20  # n_star 顺带刷新窗口（占位 = N_neglect）
    stale_after_seconds: float = 300.0  # Dead Man's Switch 窗口：超过无心跳即告警


DEFAULT_SWEEP_CONFIG = SweepConfig()
