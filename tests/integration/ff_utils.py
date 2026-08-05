"""FF 现算基准——q4 断言用具，委托 M3 引擎（lethefield_rms.ff）单一事实源。

本模块只保留 q4 测试的旧接口形态（模块级常量 + 无 config 参数的函数签名），
公式实现一律走 M3 FF 引擎，不再内嵌副本。
"""

from lethefield_rms.ff import DEFAULT_CONFIG
from lethefield_rms.ff import s_effective as _engine_s_effective
from lethefield_rms.ff import theta_effective as _engine_theta_effective

# 测试用参数（占位值，正式标定属种子期；选择依据：s=0.9、Δn=20 时 s_effective≈0.10，
# 复刻 spike Q4 已验证的衰减过滤场景）——取值与 FFConfig 默认占位保持同源
LAMBDA = DEFAULT_CONFIG.lambda_decay
T_OVER_T0 = DEFAULT_CONFIG.t_over_t0
THETA_BASE = DEFAULT_CONFIG.theta_base
RHO = 1.0


def s_effective(s: float, n_last_touched: int, n_now: int) -> float:
    """s_effective = s × e^(−λ·Δn·log(1+t/t₀))，Δn = n_now − n_last_touched。"""
    return _engine_s_effective(s, n_last_touched, n_now)


def theta_effective() -> float:
    """θ_effective = θ_base / ρ。"""
    return _engine_theta_effective(THETA_BASE, RHO)
