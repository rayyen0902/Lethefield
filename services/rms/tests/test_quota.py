"""M13 红线 2 配额单元测试：check_quota 纯函数矩阵、QuotaCounters TTL 缓存、
writer/vectors 配额触发（伪 counters 注入，无栈）。"""

import pytest
from lethefield_clients.control_plane import Tier
from lethefield_rms import vectors, writer
from lethefield_rms.quota import (
    DEFAULT_QUOTA_CONFIG,
    QuotaConfig,
    QuotaCounters,
    QuotaExceeded,
    check_quota,
    quota_for_tier,
)

_SMALL = QuotaConfig(max_vertices=2, max_edges=2, max_vectors=2, count_cache_ttl_seconds=30.0)


class _FakeResult:
    def __init__(self, value):
        self._value = value

    def all(self):
        return self

    def result(self):
        return [self._value]


class FakeGremlinClient:
    """submit 计数可观测的伪 gremlin client（计数脚本返回 count，写脚本返回 ok）。"""

    def __init__(self, count=0):
        self.count = count
        self.submits: list[tuple[str, dict | None]] = []

    def submit(self, script, bindings=None):
        self.submits.append((script, bindings))
        return _FakeResult("ok" if "addV" in script or "addEdge" in script else self.count)


class FakeEs:
    def __init__(self, doc_count=0):
        self.doc_count = doc_count
        self.count_calls: list[dict] = []
        self.indexed: list[dict] = []

    def count(self, **kwargs):
        self.count_calls.append(kwargs)
        return {"count": self.doc_count}

    def index(self, **kwargs):
        self.indexed.append(kwargs)


class FakeCounters:
    """伪配额计数器（writer/vectors 触发用例注入，绕过真实 client/es）。"""

    def __init__(self, vertex=0, edge=0, vector=0):
        self._counts = {"vertex": vertex, "edge": edge, "vector": vector}

    def vertex_count(self, gname):
        return self._counts["vertex"]

    def edge_count(self, gname):
        return self._counts["edge"]

    def vector_count(self, space_id):
        return self._counts["vector"]


class TestCheckQuota:
    @pytest.mark.parametrize("kind", ["vertex", "edge", "vector"])
    def test_below_limit_passes(self, kind):
        check_quota(kind, 1, _SMALL, space_id="sp_1")  # 不抛即过

    @pytest.mark.parametrize(
        ("kind", "limit_attr"),
        [("vertex", "max_vertices"), ("edge", "max_edges"), ("vector", "max_vectors")],
    )
    def test_at_limit_raises(self, kind, limit_attr):
        with pytest.raises(QuotaExceeded) as exc_info:
            check_quota(kind, getattr(_SMALL, limit_attr), _SMALL, space_id="sp_1")
        assert exc_info.value.kind == kind
        assert exc_info.value.limit == 2

    def test_unknown_kind_rejected(self):
        with pytest.raises(ValueError, match="未知配额 kind"):
            check_quota("node", 0, _SMALL, space_id="sp_1")


class TestQuotaExceeded:
    def test_attrs_and_str(self):
        exc = QuotaExceeded("vertex", "sp_1", 100, 100)
        assert (exc.kind, exc.space_id, exc.limit, exc.actual) == ("vertex", "sp_1", 100, 100)
        assert "quota_exceeded" in str(exc)


class TestQuotaForTier:
    def test_none_and_uncovered_tier_fall_back_to_default(self):
        assert quota_for_tier(None) is DEFAULT_QUOTA_CONFIG
        for tier in Tier:
            assert quota_for_tier(tier) is DEFAULT_QUOTA_CONFIG  # 覆盖表 1.0 为空

    def test_override_hit(self, monkeypatch):
        from lethefield_rms import quota as quota_module

        override = QuotaConfig(max_vertices=10, max_edges=10, max_vectors=10)
        monkeypatch.setitem(quota_module.TIER_QUOTA_OVERRIDES, Tier.PREMIUM, override)
        assert quota_for_tier(Tier.PREMIUM) is override
        assert quota_for_tier(Tier.HOT) is DEFAULT_QUOTA_CONFIG


class TestQuotaCounters:
    def _counters(self, client, es=None, ttl=30.0, clock=None):
        config = QuotaConfig(10, 10, 10, count_cache_ttl_seconds=ttl)
        # 独立 cache 隔离（默认进程级共享缓存会跨用例污染）
        return QuotaCounters(client, es, config, clock=clock or (lambda: 0.0), cache={})

    def test_vertex_and_edge_count_cached_within_ttl(self):
        now = [1000.0]
        client = FakeGremlinClient(count=7)
        counters = self._counters(client, clock=lambda: now[0])
        assert counters.vertex_count("g1") == 7
        assert counters.vertex_count("g1") == 7
        assert counters.edge_count("g1") == 7
        assert len(client.submits) == 2  # TTL 内同 (gname, kind) 不重复 submit

    def test_cache_expires_after_ttl(self):
        now = [1000.0]
        client = FakeGremlinClient(count=7)
        counters = self._counters(client, ttl=30.0, clock=lambda: now[0])
        counters.vertex_count("g1")
        now[0] += 31.0  # 越过 TTL
        assert counters.vertex_count("g1") == 7
        assert len(client.submits) == 2

    def test_cache_keyed_by_graph(self):
        client = FakeGremlinClient(count=3)
        counters = self._counters(client)
        counters.vertex_count("g1")
        counters.vertex_count("g2")  # 不同图名不共享缓存
        assert len(client.submits) == 2

    def test_vector_count_not_cached(self):
        es = FakeEs(doc_count=5)
        counters = self._counters(None, es)
        assert counters.vector_count("sp_1") == 5
        assert counters.vector_count("sp_1") == 5
        assert len(es.count_calls) == 2  # O(1) 计数不缓存
        assert es.count_calls[0]["routing"] == "sp_1"

    def test_missing_dependency_rejected(self):
        counters = self._counters(None, None)
        with pytest.raises(ValueError, match="gremlin client"):
            counters.vertex_count("g1")
        with pytest.raises(ValueError, match="es client"):
            counters.vector_count("sp_1")


class TestWriterQuota:
    _event_kwargs = dict(
        node_key="ev_1", space_id="sp_1", content="c", tau_ms=1, ref_ex="ev_x", s=1.0, n_created=1
    )

    def test_event_node_rejected_before_write(self):
        client = FakeGremlinClient()
        with pytest.raises(QuotaExceeded):
            writer.create_event_node(
                client,
                "g1",
                quota=_SMALL,
                quota_counters=FakeCounters(vertex=2),
                **self._event_kwargs,
            )
        assert client.submits == []  # 写脚本未被执行

    def test_entity_node_rejected_before_write(self):
        client = FakeGremlinClient()
        with pytest.raises(QuotaExceeded):
            writer.create_entity_node(
                client,
                "g1",
                entity_key="e1",
                space_id="sp_1",
                quota=_SMALL,
                quota_counters=FakeCounters(vertex=2),
            )
        assert client.submits == []

    def test_edge_rejected_before_write(self):
        client = FakeGremlinClient()
        with pytest.raises(QuotaExceeded):
            writer.create_edge(
                client,
                "g1",
                space_id="sp_1",
                from_key="a",
                to_key="b",
                label="causal",
                quota=_SMALL,
                quota_counters=FakeCounters(edge=2),
            )
        assert client.submits == []

    def test_write_proceeds_below_quota(self):
        client = FakeGremlinClient()
        writer.create_event_node(
            client, "g1", quota=_SMALL, quota_counters=FakeCounters(vertex=0), **self._event_kwargs
        )
        assert len(client.submits) == 1
        writer.create_edge(
            client,
            "g1",
            space_id="sp_1",
            from_key="a",
            to_key="b",
            label="causal",
            quota=_SMALL,
            quota_counters=FakeCounters(edge=0),
        )
        assert len(client.submits) == 2


class TestVectorsQuota:
    def test_index_rejected_before_write(self):
        es = FakeEs()
        with pytest.raises(QuotaExceeded):
            vectors.index_vector(
                es,
                space_id="sp_1",
                node_key="ev_1",
                vector=[0.1, 0.2],
                quota=_SMALL,
                quota_counters=FakeCounters(vector=2),
            )
        assert es.indexed == []  # 写文档未执行

    def test_index_proceeds_below_quota(self):
        es = FakeEs(doc_count=1)
        vectors.index_vector(es, space_id="sp_1", node_key="ev_1", vector=[0.1, 0.2], quota=_SMALL)
        assert len(es.indexed) == 1  # 未注入 counters 时用传入 es 现场构造
