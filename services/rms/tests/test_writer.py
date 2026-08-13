"""writer 查询原语单测（M15）：vertex_exists / latest_event_node / temporal_edge_exists。

无栈：FakeGremlinClient 按脚本特征返回预设结果，断言脚本形态（红线 1：
has('space_id' 同串）与绑定形态（before_n 字符串绑定）。
"""

from lethefield_rms.writer import (
    latest_event_node,
    temporal_edge_exists,
    vertex_exists,
)


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def all(self):
        return self

    def result(self):
        return self._value


class FakeGremlinClient:
    """按脚本特征分发预设结果的伪 client（submit 可观测）。"""

    def __init__(self, *, exists=True, latest=None, edge_exists=False):
        self.exists = exists
        self.latest = latest  # {'node_key': ..., 'n_created': ...} 或 None
        self.edge_exists = edge_exists
        self.submits: list[tuple[str, dict]] = []

    def submit(self, script, bindings=None):
        self.submits.append((script, bindings))
        if "project" in script:
            # 真实服务端按元素逐个流回（非嵌套列表）
            return _FakeResult([] if self.latest is None else [self.latest])
        if "outE('temporal')" in script:
            return _FakeResult([self.edge_exists])
        return _FakeResult([self.exists])


def test_vertex_exists_true():
    client = FakeGremlinClient(exists=True)
    assert vertex_exists(client, "g1", space_id="sp", node_key="ev_a") is True
    script, bindings = client.submits[0]
    assert "has('space_id'" in script  # 红线 1 规则 A
    assert bindings == {"gname": "g1", "spaceId": "sp", "nodeKey": "ev_a"}


def test_vertex_exists_false():
    client = FakeGremlinClient(exists=False)
    assert vertex_exists(client, "g1", space_id="sp", node_key="ev_a") is False


def test_latest_event_node_hit():
    client = FakeGremlinClient(latest={"node_key": "ev_a", "n_created": 41})
    assert latest_event_node(client, "g1", space_id="sp") == ("ev_a", 41)
    _, bindings = client.submits[0]
    assert bindings["beforeN"] is None  # 无界 = NTracker 冷启动播种


def test_latest_event_node_before_n_string_binding():
    """before_n 以字符串绑定（gremlin_python int32 限制，Groovy 侧 as long）。"""
    client = FakeGremlinClient(latest={"node_key": "ev_a", "n_created": 40})
    assert latest_event_node(client, "g1", space_id="sp", before_n=42) == ("ev_a", 40)
    script, bindings = client.submits[0]
    assert bindings["beforeN"] == "42"
    assert "lt(beforeN as long)" in script


def test_latest_event_node_empty_graph():
    client = FakeGremlinClient(latest=None)
    assert latest_event_node(client, "g1", space_id="sp") is None


def test_temporal_edge_exists():
    client = FakeGremlinClient(edge_exists=True)
    assert temporal_edge_exists(client, "g1", space_id="sp", from_key="ev_a", to_key="ev_b") is True
    script, bindings = client.submits[0]
    assert "has('space_id'" in script
    assert "outE('temporal')" in script
    assert bindings["fromKey"] == "ev_a" and bindings["toKey"] == "ev_b"


def test_temporal_edge_missing():
    client = FakeGremlinClient(edge_exists=False)
    assert (
        temporal_edge_exists(client, "g1", space_id="sp", from_key="ev_a", to_key="ev_b") is False
    )
