"""DMS 配置（M10，§20 待标定占位——全部可配、禁硬编码）。

字段默认值即占位初值；env 覆盖前缀 `LETHEFIELD_DMS_`
（如 LETHEFIELD_DMS_STALE_THRESHOLD_SECONDS）。
Pulsar admin URL 沿用全局约定 env LETHEFIELD_PULSAR_ADMIN_URL（与调度器同源）。
"""

import os
from dataclasses import dataclass

ENV_PREFIX = "LETHEFIELD_DMS_"


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(ENV_PREFIX + name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(ENV_PREFIX + name, default))


@dataclass(frozen=True)
class DmsConfig:
    """DMS 巡检参数（占位初值，正式标定属种子期参数标定流程）。"""

    # 探针收发超时：超过即判定管道不活性（page 级）
    probe_timeout_ms: int = 5_000
    # 活跃窗口 W：last_write 在过去 W 内的 space 纳入监控（冷 space 长期不写不打扰）
    activity_window_seconds: float = 7 * 24 * 3600
    # 新鲜度阈值：活跃 space 的 last_write 距今超过此值 → stale
    stale_threshold_seconds: float = 24 * 3600
    # 训练控制 backlog 持续非零超过此值 → page 级告警（consumer 停摆 = 合规失效）
    backlog_stale_seconds: float = 300
    # 巡检循环节奏
    loop_interval_seconds: float = 60
    # Pulsar admin REST（tenant/namespace 幂等确保 + topic stats 查询）
    pulsar_admin_url: str = "http://localhost:8080"

    @classmethod
    def from_env(cls) -> "DmsConfig":
        return cls(
            probe_timeout_ms=_env_int("PROBE_TIMEOUT_MS", cls.probe_timeout_ms),
            activity_window_seconds=_env_float(
                "ACTIVITY_WINDOW_SECONDS", cls.activity_window_seconds
            ),
            stale_threshold_seconds=_env_float(
                "STALE_THRESHOLD_SECONDS", cls.stale_threshold_seconds
            ),
            backlog_stale_seconds=_env_float("BACKLOG_STALE_SECONDS", cls.backlog_stale_seconds),
            loop_interval_seconds=_env_float("LOOP_INTERVAL_SECONDS", cls.loop_interval_seconds),
            pulsar_admin_url=os.environ.get("LETHEFIELD_PULSAR_ADMIN_URL", cls.pulsar_admin_url),
        )


DEFAULT_CONFIG = DmsConfig()
