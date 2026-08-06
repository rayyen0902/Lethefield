"""M2 schema 常量与 groovy 资源的一致性单测——不需要栈。

schema 元素名经绑定传入 groovy 脚本，脚本头注释（property-keys / edge-labels /
indexes 三行）是与常量比对的可解析清单；两侧漂移在此拦截。
"""

import re

from lethefield_rms.schema import (
    COMPOSITE_INDEXES,
    EDGE_LABELS,
    EXPECTED_PROPERTY_KEYS,
    ensure_schema_script,
)
from lethefield_rms.vectors import vectors_mapping

ALLOWED_TYPES = {"String", "Date", "Double", "Long", "Integer"}


def test_groovy_lists_all_schema_elements():
    script = ensure_schema_script()
    for name in EXPECTED_PROPERTY_KEYS:
        assert name in script, f"groovy 缺少属性键 {name}"
    for label in EDGE_LABELS:
        assert label in script, f"groovy 缺少边标签 {label}"
    for index_name, _, _ in COMPOSITE_INDEXES:
        assert index_name in script, f"groovy 缺少索引 {index_name}"


def test_groovy_has_no_forbidden_items():
    script = ensure_schema_script()
    assert "ids.authority.wait-time" not in script
    assert "drop" not in script.lower()


def test_property_key_types_are_supported():
    for name, type_name in EXPECTED_PROPERTY_KEYS.items():
        assert type_name in ALLOWED_TYPES, f"{name} 的类型 {type_name} 不在支持集内"
    # groovy 侧 types 映射必须覆盖全部用到的类型简单名
    script = ensure_schema_script()
    for type_name in set(EXPECTED_PROPERTY_KEYS.values()):
        assert f"{type_name}: {type_name}.class" in script


def test_groovy_property_keys_match_constants():
    """解析 groovy 头注释的 property-keys 清单，与 EXPECTED_PROPERTY_KEYS 逐项一致。"""
    script = ensure_schema_script()
    pattern = r"(?<![=<>!])\b([a-z_]\w*)=(String|Date|Double|Long|Integer)\b"
    declared = dict(re.findall(pattern, script))
    assert declared == EXPECTED_PROPERTY_KEYS


def test_groovy_edge_labels_and_indexes_match_constants():
    script = ensure_schema_script()
    edge_line = next(line for line in script.splitlines() if "edge-labels:" in line)
    declared_edges = tuple(e.strip() for e in edge_line.split("edge-labels:")[1].split(","))
    assert declared_edges == EDGE_LABELS

    index_line = next(line for line in script.splitlines() if "indexes:" in line)
    declared_indexes = re.findall(r"(\w+)\((\w+)(, unique)?\)", index_line)
    assert [(n, k, bool(u)) for n, k, u in declared_indexes] == list(COMPOSITE_INDEXES)


def test_vectors_mapping_structure():
    mapping = vectors_mapping(4)
    props = mapping["properties"]
    assert props["node_key"] == {"type": "keyword"}
    assert props["space_id"] == {"type": "keyword"}
    assert props["v"] == {
        "type": "dense_vector",
        "dims": 4,
        "index": True,
        "similarity": "cosine",
    }


# ---------------------------------------------------------------- M7 红线：禁失效标志


def test_schema_has_no_invalidation_flags():
    """M7 红线：节点/边属性中不存在任何"硬失效标志"字段（开发文档 §8 验收项 1）。"""
    from lethefield_rms.schema import find_invalidation_flags

    assert find_invalidation_flags(list(EXPECTED_PROPERTY_KEYS)) == []
    assert find_invalidation_flags(list(EDGE_LABELS)) == []  # supersedes 是边不是标志
    # 谓词本身有效：典型违规命名能被拦下
    assert find_invalidation_flags(["tombstone", "is_invalidated", "superseded_by"]) == [
        "tombstone",
        "is_invalidated",
        "superseded_by",
    ]
