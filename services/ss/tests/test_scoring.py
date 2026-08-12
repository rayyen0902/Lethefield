"""scoring 编排单测（M14）：权重来自配置、降级两级规则、clamp、ScoringError 路径。"""

import pytest
from lethefield_clients.ex_stream import ExStreamEvent
from lethefield_ss.config import DEGRADE_RETRY, SSConfig
from lethefield_ss.llm import ScoringError
from lethefield_ss.scoring import compose_s, score_event


class FakeScorer:
    """按预设响应原文应答的 scorer（协议：score(content) -> (raw, usage, model)）。"""

    def __init__(self, raw: str, model: str = "fake-v1") -> None:
        self.raw = raw
        self.model = model
        self.calls: list[str] = []

    def score(self, content: str):
        self.calls.append(content)
        return self.raw, {"prompt_tokens": 10, "completion_tokens": 5}, self.model


def _event() -> ExStreamEvent:
    return ExStreamEvent(
        space_id="demo",
        event_id="e1",
        n=1,
        content="内容",
        agent_actor_id="a",
        account_id="acc",
        tau_ms=None,
        ref_conflict=None,
        created_at_ms=1,
    )


FULL = '{"er": 0.6, "e": 0.0, "i": 0.6, "g": 0.6, "n": 0.6, "c": 0.6}'
ONE_MISSING = '{"er": 0.6, "e": 0.0, "i": 0.6, "g": 0.6, "n": 0.6}'
TWO_MISSING = '{"er": 0.6, "e": 0.0, "i": 0.6, "g": 0.6}'


def test_compose_s_uses_config_weights():
    """权重来自配置而非代码常量：改配置直接改变合成结果。"""
    dims = {d: 0.5 for d in ("er", "e", "i", "g", "n", "c")}
    equal = SSConfig()
    assert compose_s(dims, equal.weights) == pytest.approx(0.5)
    heavy_er = SSConfig(weights={"er": 1.0, "e": 0, "i": 0, "g": 0, "n": 0, "c": 0})
    assert compose_s(dims, heavy_er.weights) == pytest.approx(0.5 * 1.0)


def test_compose_s_clamped():
    dims = {d: 1.0 for d in ("er", "e", "i", "g", "n", "c")}
    weights = {d: 1.0 for d in ("er", "e", "i", "g", "n", "c")}
    assert compose_s(dims, weights) == 1.0  # Σ=6 → clamp


def test_score_event_full():
    result, usage = score_event(_event(), scorer=FakeScorer(FULL), config=SSConfig())
    assert result.degraded is False and result.missing_dims == []
    assert result.s == pytest.approx(0.5)  # (0.6*5 + 0)/6
    assert result.node_key == "ev_e1"  # node_key 单点规则
    assert result.model_version == "fake-v1"
    assert usage["prompt_tokens"] == 10


def test_score_event_one_missing_neutral_mark():
    """缺 1 维 + neutral_mark（默认）：中性值 + degraded + 缺失维清单。"""
    result, _ = score_event(_event(), scorer=FakeScorer(ONE_MISSING), config=SSConfig())
    assert result.degraded is True and result.missing_dims == ["c"]
    assert result.dims["c"] == 0.5  # degrade_neutral 默认
    assert result.s == pytest.approx((0.6 * 4 + 0.0 + 0.5) / 6)


def test_score_event_neutral_value_configurable():
    config = SSConfig(degrade_neutral=0.3)
    result, _ = score_event(_event(), scorer=FakeScorer(ONE_MISSING), config=config)
    assert result.dims["c"] == 0.3


def test_score_event_two_missing_fails():
    """缺 ≥2 维：不是降级是失败（合成 s 无意义）。"""
    with pytest.raises(ScoringError, match="缺 2 维"):
        score_event(_event(), scorer=FakeScorer(TWO_MISSING), config=SSConfig())


def test_score_event_retry_policy_fails_on_one_missing():
    """SS_DEGRADE_POLICY=retry：缺 1 维也整单失败。"""
    config = SSConfig(degrade_policy=DEGRADE_RETRY)
    with pytest.raises(ScoringError):
        score_event(_event(), scorer=FakeScorer(ONE_MISSING), config=config)


def test_score_event_unparseable_fails():
    with pytest.raises(ScoringError, match="不可解析"):
        score_event(_event(), scorer=FakeScorer("我拒绝回答"), config=SSConfig())
