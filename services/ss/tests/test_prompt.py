"""prompt 解析单测（M14）：正常/缺维/非 JSON/越界/噪声包裹的严格分级。"""

import pytest
from lethefield_ss.prompt import parse_scores

FULL = '{"er": 0.9, "e": 0.1, "i": 0.8, "g": 0.3, "n": 0.6, "c": 0.0}'


def test_parse_full_scores():
    dims, missing = parse_scores(FULL)
    assert dims == {"er": 0.9, "e": 0.1, "i": 0.8, "g": 0.3, "n": 0.6, "c": 0.0}
    assert missing == []


def test_parse_with_surrounding_noise():
    """模型夹带解释文字：提取 JSON 块（可解析面的宽松仅限于此）。"""
    dims, missing = parse_scores(f"打分如下：\n{FULL}\n以上是结果。")
    assert dims["er"] == 0.9 and missing == []


def test_parse_one_missing_dim():
    dims, missing = parse_scores('{"er": 0.9, "e": 0.1, "i": 0.8, "g": 0.3, "n": 0.6}')
    assert "c" in missing and len(missing) == 1
    assert "c" not in dims


def test_parse_two_missing_dims():
    _, missing = parse_scores('{"er": 0.9, "e": 0.1, "i": 0.8, "g": 0.3}')
    assert sorted(missing) == ["c", "n"]


def test_parse_out_of_range_counts_as_missing():
    """越界值视为该维缺失（字段级缺陷走降级面，不静默 clamp）。"""
    _, missing = parse_scores('{"er": 1.7, "e": 0.1, "i": 0.8, "g": 0.3, "n": 0.6, "c": -0.2}')
    assert sorted(missing) == ["c", "er"]


def test_parse_non_numeric_counts_as_missing():
    _, missing = parse_scores('{"er": "high", "e": 0.1, "i": 0.8, "g": 0.3, "n": 0.6, "c": 0.1}')
    assert missing == ["er"]


@pytest.mark.parametrize("raw", ["", "无法打分", "[1, 2, 3]", "{"])
def test_parse_unparseable_raises(raw):
    """整体不可解析 → ValueError（失败路径，非降级面）。"""
    with pytest.raises(ValueError):
        parse_scores(raw)
