"""M12 exporter 运行配置（env 可配占位，阈值待标定，禁硬编码）。"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ExporterConfig:
    # 轮询节奏（秒）
    poll_interval_seconds: float = 60.0
    # ff_recalled_then_touched_rate 的关联窗 T（毫秒；§19.2 初值 24h）
    touched_window_ms: int = 86_400_000
    # graph_lru_cache_hit_ratio 代理口径：同 space 两次召回间隔超过该值视为"闲置后首请求"
    lru_idle_gap_ms: int = 1_800_000  # 30min 占位
    # …且 Stage 总耗时超过该阈值视为"缓存失效信号"（毫秒）
    lru_slow_stage_ms: float = 500.0  # 占位
    # /metrics 暴露口端口（M12 端口约定）
    metrics_port: int = 9104
    # checkpoint 状态文件
    state_path: str = "var/metrics_exporter/state.json"

    @classmethod
    def from_env(cls) -> "ExporterConfig":
        return cls(
            poll_interval_seconds=float(
                os.environ.get("LETHEFIELD_EXPORTER_POLL_SECONDS", cls.poll_interval_seconds)
            ),
            touched_window_ms=int(
                os.environ.get("LETHEFIELD_EXPORTER_TOUCHED_WINDOW_MS", cls.touched_window_ms)
            ),
            lru_idle_gap_ms=int(
                os.environ.get("LETHEFIELD_EXPORTER_LRU_IDLE_GAP_MS", cls.lru_idle_gap_ms)
            ),
            lru_slow_stage_ms=float(
                os.environ.get("LETHEFIELD_EXPORTER_LRU_SLOW_STAGE_MS", cls.lru_slow_stage_ms)
            ),
            state_path=os.environ.get("LETHEFIELD_EXPORTER_STATE_PATH", cls.state_path),
        )


DEFAULT_CONFIG = ExporterConfig()
