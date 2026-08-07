"""M11 运行配置（env 可配占位，禁硬编码；阈值进 §20 待标定）。"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TrainingConfig:
    # 热层落盘根目录（1.0 无对象存储，本地目录为载体；目录布局见 hot_store）
    hot_root: str = "var/training"
    # R3 关联时间窗（召回明细 × 纠错对命中窗口，毫秒；§20 待标定）
    w_r3_ms: int = 86_400_000  # 24h 占位
    # 热层滚动保留天数（近 90 天，§12.4 分层定案）
    hot_retention_days: int = 90
    # consumer receive 轮询超时（毫秒）
    receive_timeout_ms: int = 1000

    @classmethod
    def from_env(cls) -> "TrainingConfig":
        return cls(
            hot_root=os.environ.get("LETHEFIELD_TRAINING_HOT_ROOT", cls.hot_root),
            w_r3_ms=int(os.environ.get("LETHEFIELD_TRAINING_W_R3_MS", cls.w_r3_ms)),
            hot_retention_days=int(
                os.environ.get("LETHEFIELD_TRAINING_HOT_RETENTION_DAYS", cls.hot_retention_days)
            ),
        )


DEFAULT_CONFIG = TrainingConfig()
