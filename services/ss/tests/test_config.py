"""config 单测（M14）：LLM env fail-closed、权重覆盖校验、降级参数校验。"""

import pytest
from lethefield_ss.config import SSConfig


def test_from_env_missing_llm_fail_closed(monkeypatch):
    """LLM 三变量缺失即明确报错（不静默降级）。"""
    for var in ("SS_LLM_BASE_URL", "SS_LLM_API_KEY", "SS_LLM_MODEL"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ValueError, match="LLM 配置不全"):
        SSConfig.from_env()
    with pytest.raises(ValueError, match="LLM 配置不全"):
        SSConfig.from_env(require_llm=True)


def test_from_env_without_llm_requirement(monkeypatch):
    for var in ("SS_LLM_BASE_URL", "SS_LLM_API_KEY", "SS_LLM_MODEL"):
        monkeypatch.delenv(var, raising=False)
    config = SSConfig.from_env(require_llm=False)
    assert config.llm_model == ""


def test_from_env_reads_llm_vars(monkeypatch):
    monkeypatch.setenv("SS_LLM_BASE_URL", "https://api.example.com/v1")
    monkeypatch.setenv("SS_LLM_API_KEY", "sk-test")
    monkeypatch.setenv("SS_LLM_MODEL", "test-model")
    config = SSConfig.from_env()
    assert config.llm_base_url == "https://api.example.com/v1"
    assert config.llm_model == "test-model"


def test_weights_default_equal():
    config = SSConfig()
    assert len(config.weights) == 6
    assert sum(config.weights.values()) == pytest.approx(1.0)


def test_weights_env_override(monkeypatch):
    monkeypatch.setenv("SS_LLM_BASE_URL", "x")
    monkeypatch.setenv("SS_LLM_API_KEY", "x")
    monkeypatch.setenv("SS_LLM_MODEL", "x")
    monkeypatch.setenv(
        "LETHEFIELD_SS_WEIGHTS_JSON",
        '{"er": 0.4, "e": 0.2, "i": 0.1, "g": 0.1, "n": 0.1, "c": 0.1}',
    )
    config = SSConfig.from_env()
    assert config.weights["er"] == 0.4


def test_weights_key_mismatch_rejected():
    with pytest.raises(ValueError, match="权重键不符"):
        SSConfig(weights={"er": 1.0})


def test_negative_weight_rejected():
    bad = {d: 0.2 for d in ("er", "e", "i", "g", "n")}
    bad["c"] = -0.2
    with pytest.raises(ValueError, match="非负"):
        SSConfig(weights=bad)


def test_unknown_degrade_policy_rejected():
    with pytest.raises(ValueError, match="未知降级策略"):
        SSConfig(degrade_policy="yolo")


def test_degrade_neutral_range_checked():
    with pytest.raises(ValueError, match="越界"):
        SSConfig(degrade_neutral=1.5)
