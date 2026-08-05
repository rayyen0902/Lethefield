"""M3 FF 计算引擎单元测试（开发文档 §4 验收标准 1）。

覆盖：s_effective 与公式手算一致（含边界 Δn=0、λ=0）、s 触顶/触底截断、
n*/绝对遗忘视界、θ_effective、参数分层归属。图侧 δ 落库由集成测试覆盖。
"""

import math

import pytest
from lethefield_rms.ff import (
    DEFAULT_CONFIG,
    FFConfig,
    clamp_s,
    n_star,
    n_star_horizon,
    s_effective,
    theta_effective,
)
from prometheus_client import REGISTRY

# 与 tests/integration/ff_utils.py（spike 复刻场景）同源的占位参数
LAMBDA = DEFAULT_CONFIG.lambda_decay  # 0.16
T_OVER_T0 = DEFAULT_CONFIG.t_over_t0  # 1.0


def _hand_calc(s: float, delta_n: int) -> float:
    """公式手算基准：s × e^(−λ·Δn·log(1+t/t₀))。"""
    return s * math.exp(-LAMBDA * delta_n * math.log(1 + T_OVER_T0))


class TestSEffective:
    @pytest.mark.parametrize(
        ("s", "n_last_touched", "n_now"),
        [
            (0.9, 100, 100),  # Δn=0：无衰减
            (0.9, 100, 101),  # Δn=1
            (0.9, 100, 120),  # spike 场景 Δn=20 → s_eff≈0.098
            (0.5, 0, 7),
            (1.0, 50, 60),  # s 触顶值照常衰减
            (0.0, 10, 30),  # s=0 恒 0
        ],
    )
    def test_matches_hand_calc(self, s, n_last_touched, n_now):
        expected = _hand_calc(s, n_now - n_last_touched)
        assert s_effective(s, n_last_touched, n_now) == pytest.approx(expected)

    def test_delta_n_zero_is_no_decay(self):
        assert s_effective(0.9, 100, 100) == pytest.approx(0.9)

    def test_lambda_zero_is_no_decay(self):
        config = FFConfig(lambda_decay=0.0)
        assert s_effective(0.9, 100, 999, config=config) == pytest.approx(0.9)

    def test_spike_decay_scenario(self):
        """spike Q4 已验证场景：s=0.9、Δn=20 → s_effective≈0.10，被 θ=0.3 过滤。"""
        eff = s_effective(0.9, 80, 100)
        assert 0.05 < eff < 0.15
        assert eff < theta_effective(DEFAULT_CONFIG.theta_base, rho=1.0)


class TestThetaEffective:
    def test_rho_one(self):
        assert theta_effective(0.3, 1.0) == pytest.approx(0.3)

    def test_rho_scales_down(self):
        # ρ 增大 → θ_effective 降低 → 召回放宽（ρ 只作用于两处硬过滤，见 M4）
        assert theta_effective(0.3, 2.0) == pytest.approx(0.15)


class TestNStar:
    def test_matches_formula(self):
        # n* = ln(s/θ) / (λ·ln(1+t/t₀)) = ln(3) / (0.16·ln2)
        expected = math.log(0.9 / 0.3) / (0.16 * math.log(2))
        assert n_star(0.9, 0.3) == pytest.approx(expected)

    def test_below_theta_is_zero(self):
        assert n_star(0.3, 0.3) == 0.0
        assert n_star(0.1, 0.3) == 0.0

    def test_lambda_zero_is_infinite(self):
        config = FFConfig(lambda_decay=0.0)
        assert n_star(0.9, 0.3, config=config) == math.inf


class TestNStarHorizon:
    def test_absolute_horizon_is_n_last_touched_plus_n_star(self):
        # 绝对视界 = n_last_touched + ceil(n*)——粗筛与绝对事件序号 n_now 可比
        expected = 100 + math.ceil(n_star(0.9, 0.3))
        assert n_star_horizon(0.9, 100, 0.3) == expected

    def test_ceil_never_undershoots(self):
        # ceil 取整：粗筛宁可多留不可误杀，视界不小于精确值
        horizon = n_star_horizon(0.9, 100, 0.3)
        assert horizon >= 100 + n_star(0.9, 0.3)

    def test_lambda_zero_clamps_to_long_max(self):
        config = FFConfig(lambda_decay=0.0)
        assert n_star_horizon(0.9, 100, 0.3, config=config) == 2**63 - 1


class TestClampS:
    def _clamp_metric(self, bound: str) -> float:
        return REGISTRY.get_sample_value("lethefield_ff_s_clamp_total", {"bound": bound}) or 0.0

    def test_within_range_unchanged(self):
        assert clamp_s(0.5) == (0.5, None)

    def test_upper_bound_clamped_and_counted(self):
        before = self._clamp_metric("upper")
        assert clamp_s(1.1) == (1.0, "upper")  # 0.95 + 0.2 强化 → 触顶
        assert self._clamp_metric("upper") == before + 1

    def test_lower_bound_clamped_and_counted(self):
        before = self._clamp_metric("lower")
        assert clamp_s(-0.45) == (0.0, "lower")  # 0.05 − 0.5 冲突 → 触底
        assert self._clamp_metric("lower") == before + 1

    def test_exact_bounds_not_clamped(self):
        assert clamp_s(1.0) == (1.0, None)
        assert clamp_s(0.0) == (0.0, None)


class TestParameterLayering:
    def test_config_holds_agent_level_constants_only(self):
        # 参数分层：λ / N_neglect / t/t₀ 属 agent 常数层；ρ 是查询时参数，不进 config
        assert not hasattr(DEFAULT_CONFIG, "rho")
        assert DEFAULT_CONFIG.lambda_decay == 0.16
        assert DEFAULT_CONFIG.n_neglect == 20

    def test_config_is_frozen(self):
        with pytest.raises(AttributeError):
            DEFAULT_CONFIG.lambda_decay = 0.2  # type: ignore[misc]
