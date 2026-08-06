"""spaces 单测：space_id 字符集 fail-closed（M8 定案单点）与 SpaceType 枚举。"""

import pytest
from lethefield_clients import SPACE_ID_MAX_LEN, SpaceType, validate_space_id


@pytest.mark.parametrize(
    "ok",
    [
        "demo",
        "a1_b2",
        "9lives",  # 数字开头合法（keyspace 有 ex_ 前缀、图名交给 JanusGraph）
        "_",
        "x" * SPACE_ID_MAX_LEN,  # 边界：恰好 40
    ],
)
def test_validate_space_id_ok(ok):
    assert validate_space_id(ok) == ok


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "A",  # 大写
        "has-dash",
        "has space",
        "中文空间",
        "x" * (SPACE_ID_MAX_LEN + 1),  # 41 字符
    ],
)
def test_validate_space_id_fail_closed(bad):
    with pytest.raises(ValueError, match="命名约束"):
        validate_space_id(bad)


def test_space_type_values():
    assert SpaceType.COMPANION == "companion"
    assert SpaceType.PROJECT == "project"
