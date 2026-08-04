"""FF 现算纯函数——集成测试的断言用具。

注意：这不是 RMS 业务代码。FF 引擎的正式实现属 M3；
此处仅为 q4 断言（高分召回/低分过滤/衰减过滤）提供与公式一致的手算基准。
"""

import math

# 测试用参数（占位值，正式标定属种子期；选择依据：s=0.9、Δn=20 时 s_effective≈0.10，
# 复刻 spike Q4 已验证的衰减过滤场景）
LAMBDA = 0.16
T_OVER_T0 = 1.0  # t/t₀
THETA_BASE = 0.3
RHO = 1.0


def s_effective(s: float, n_last_touched: int, n_now: int) -> float:
    """s_effective = s × e^(−λ·Δn·log(1+t/t₀))，Δn = n_now − n_last_touched。"""
    delta_n = n_now - n_last_touched
    return s * math.exp(-LAMBDA * delta_n * math.log(1 + T_OVER_T0))


def theta_effective() -> float:
    """θ_effective = θ_base / ρ。"""
    return THETA_BASE / RHO
