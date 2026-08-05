"""M5 service 层单元测试：四操作（fake 依赖）、debug 裁剪（字段级）、EX n 分配。

图/EX/Redis 全部用 fake——service 层逻辑（判定、盖章、裁剪、参数流转）不依赖真实存储；
真实存储的端到端由 tests/integration/test_m5_api.py 覆盖。
"""

from types import SimpleNamespace

import pytest
from lethefield_api import ex_ingest, service
from lethefield_api.auth import Claims
from lethefield_api.errors import ApiError, ErrorCode
from lethefield_rms import ff
from lethefield_rms.retrieve import EdgeRecord, NodeItem, RetrievalResult


class FakeRedis:
    def __init__(self):
        self._data: dict[str, int] = {}

    def incr(self, key: str) -> int:
        self._data[key] = self._data.get(key, 0) + 1
        return self._data[key]

    def get(self, key: str):
        v = self._data.get(key)
        return str(v).encode() if v is not None else None

    def set(self, key: str, value: int) -> None:
        self._data[key] = value


class FakeSession:
    def __init__(self, max_n=0):
        self.executed: list = []
        self._max_n = max_n

    def execute(self, query, params=None):
        self.executed.append((query, params))
        return SimpleNamespace(one=lambda: SimpleNamespace(mx=self._max_n))


def _ctx(**overrides) -> service.ApiContext:
    ctx = service.ApiContext(
        gremlin=None,
        es=None,
        ex_session=FakeSession(),
        redis=FakeRedis(),
        meta_appender=lambda **kw: None,
    )
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx


def _claims(scopes=("record", "reinforce", "flag_conflict", "retrieve")) -> Claims:
    return Claims("acct-1", ("sp_1",), "claude-code", scopes)


class TestRecord:
    def test_appends_experience_with_claim_actor(self):
        ctx = _ctx()
        result = service.record(ctx, _claims(), space_id="sp_1", content="hello")
        assert result["n"] == 1
        query, params = ctx.ex_session.executed[-1]
        assert "experience_events" in query
        # 盖章字段来自 claim，不是请求参数
        assert params[3] == "claude-code"
        assert params[4] == "acct-1"

    def test_scope_and_space_enforced(self):
        ctx = _ctx()
        with pytest.raises(ApiError) as exc:
            service.record(ctx, _claims(scopes=("retrieve",)), space_id="sp_1", content="x")
        assert exc.value.code == ErrorCode.FORBIDDEN_SCOPE
        with pytest.raises(ApiError) as exc:
            service.record(ctx, _claims(), space_id="sp_2", content="x")
        assert exc.value.code == ErrorCode.FORBIDDEN_SPACE

    def test_flag_conflict_carries_ref(self):
        ctx = _ctx()
        service.flag_conflict(
            ctx, _claims(), space_id="sp_1", content="correct info", ref_conflict="node-old"
        )
        _, params = ctx.ex_session.executed[-1]
        assert params[6] == "node-old"  # ref_conflict 落 EX（可从 EX 重放推导 supersedes）


class TestReinforce:
    def test_applies_delta_and_appends_meta(self, monkeypatch):
        appended = []
        ctx = _ctx(meta_appender=lambda **kw: appended.append(kw))
        state = ff.PhiState(0.7, 100, 110, 1, 0, 0)
        monkeypatch.setattr(service.ff, "apply_reinforce", lambda *a, **kw: state)

        result = service.reinforce(ctx, _claims(), space_id="sp_1", node_key="n1")

        assert result == {"node_key": "n1", "applied": True}  # 默认不含 φ 字段
        assert appended[0]["meta_type"] == "reinforce"
        assert appended[0]["n_at_event"] == 0  # FakeRedis 初始 n_now=0
        assert appended[0]["agent_actor_id"] == "claude-code"

    def test_debug_scope_attaches_phi(self, monkeypatch):
        ctx = _ctx()
        state = ff.PhiState(0.7, 100, 110, 1, 0, 0)
        monkeypatch.setattr(service.ff, "apply_reinforce", lambda *a, **kw: state)
        debug_claims = Claims("acct-1", ("sp_1",), "claude-code", ("reinforce", "debug"))

        result = service.reinforce(ctx, debug_claims, space_id="sp_1", node_key="n1")

        assert result["phi"]["s"] == 0.7
        assert result["phi"]["reinforce_count"] == 1


class TestRetrievePresent:
    def _result(self) -> RetrievalResult:
        return RetrievalResult(
            nodes=[NodeItem("n1", "content", None, 0.9, 0.8, False)],
            edges=[EdgeRecord("n1", "n2", "temporal")],
        )

    def test_default_crops_ff_fields(self, monkeypatch):
        ctx = _ctx()
        monkeypatch.setattr(service, "rms_retrieve", lambda *a, **kw: self._result())

        response = service.retrieve(ctx, _claims(), space_id="sp_1", query_text="q")

        (node,) = response["nodes"]
        # 字段级断言：不是"看起来没有"，是键不存在
        assert set(node) == {"node_key", "content", "tau", "brief"}
        assert "s_effective" not in node and "phi" not in node
        assert response["edges"] == [{"out_key": "n1", "in_key": "n2", "label": "temporal"}]

    def test_debug_scope_attaches_phi(self, monkeypatch):
        ctx = _ctx()
        monkeypatch.setattr(service, "rms_retrieve", lambda *a, **kw: self._result())
        monkeypatch.setattr(
            service.ff, "read_phi", lambda *a, **kw: ff.PhiState(0.9, 100, 110, 2, 0, 1)
        )
        debug_claims = Claims("acct-1", ("sp_1",), "claude-code", ("retrieve", "debug"))

        response = service.retrieve(ctx, debug_claims, space_id="sp_1", query_text="q")

        (node,) = response["nodes"]
        assert node["s_effective"] == 0.9
        assert node["phi"]["n_star_cached"] == 110
        assert node["phi"]["neglect_count"] == 1


class TestNNow:
    def test_cached_read(self):
        redis = FakeRedis()
        redis.set("ex:n:sp_1", 42)
        assert ex_ingest.n_now(redis, FakeSession(max_n=99), space_id="sp_1") == 42

    def test_rebuild_from_ex_on_cache_miss(self):
        redis = FakeRedis()
        assert ex_ingest.n_now(redis, FakeSession(max_n=37), space_id="sp_1") == 37
        assert redis.get("ex:n:sp_1") == b"37"  # 重建后回写缓存


class TestKeyspaceName:
    def test_valid(self):
        assert ex_ingest.keyspace_name("sp_1") == "ex_sp_1"

    @pytest.mark.parametrize("bad", ["", "Sp_1", "sp-1", "x" * 41])
    def test_invalid_rejected(self, bad):
        # fail-closed：不静默改写（防两个 space 映射到同一 keyspace）
        with pytest.raises(ValueError):
            ex_ingest.keyspace_name(bad)


class TestMcpRegistration:
    def test_four_tools_registered(self):
        from lethefield_api.mcp_server import create_mcp_server

        mcp = create_mcp_server(_ctx())
        tools = getattr(mcp._tool_manager, "_tools", {})
        assert set(tools) == {
            "memory_record",
            "memory_flag_conflict",
            "memory_reinforce",
            "memory_retrieve",
        }
