"""M5 MCP / SDK 接口层验收的集成测试（开发文档 §6 五条验收标准）。

真实组件：cassandra-ex（EX 摄入）、Redis（n 计数）、JanusGraph（检索/reinforce）、
ES（向量）；HTTP 层走 httpx ASGITransport（不起真实端口）。

场景：space 图（gname = space_id 约定）两个事件节点 hot/other + temporal 边；
redis n 计数基线 100。
"""

import contextlib
import os
import socket
import threading
import time
import uuid
from types import SimpleNamespace

import httpx
import jwt
import pytest
import uvicorn

os.environ["LETHEFIELD_JWT_SECRET"] = "m5-integration-secret"  # 必须在导入 api 前设定
TEST_SECRET = "m5-integration-secret"

from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL  # noqa: E402
from lethefield_api.errors import ApiError  # noqa: E402
from lethefield_api.ex_ingest import ensure_ex_keyspace, keyspace_name  # noqa: E402
from lethefield_api.http_app import create_app  # noqa: E402
from lethefield_api.sdk import MemoryClient  # noqa: E402
from lethefield_api.service import ApiContext, _make_background_appender  # noqa: E402
from lethefield_clients import (  # noqa: E402
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    redis_client,
)
from lethefield_rms.ff import read_phi  # noqa: E402
from lethefield_rms.schema import ensure_graph_schema  # noqa: E402
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector  # noqa: E402
from lethefield_rms.writer import create_edge, create_event_node  # noqa: E402

SPACE = f"m5_{uuid.uuid4().hex[:8]}"  # [a-z0-9_] 命名约束（EX keyspace）
N_BASE = 100
TAU = 1_720_000_000_000


def _token(scopes, space_ids=(SPACE,), actor="claude-code", exp=None, secret=TEST_SECRET):
    return jwt.encode(
        {
            "account_id": "acct-m5",
            "space_id": list(space_ids),
            "agent_actor_id": actor,
            "scope": list(scopes),
            "exp": exp if exp is not None else int(time.time()) + 600,
        },
        secret,
        algorithm="HS256",
    )


FULL_TOKEN = _token(("record", "reinforce", "flag_conflict", "retrieve"))
DEBUG_TOKEN = _token(("retrieve", "reinforce", "debug"))


@pytest.fixture(scope="module")
def stack():
    gremlin = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(gremlin, SPACE)
    ex_session = ex_cassandra_cluster().connect()
    ensure_ex_keyspace(ex_session, SPACE)
    redis = redis_client()
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)

    create_event_node(
        gremlin,
        SPACE,
        node_key="hot",
        space_id=SPACE,
        content="m5 integration memory",
        tau_ms=TAU,
        ref_ex="ex-hot",
        s=0.5,
        n_created=N_BASE,
    )
    create_event_node(
        gremlin,
        SPACE,
        node_key="other",
        space_id=SPACE,
        content="neighbor note",
        tau_ms=TAU,
        ref_ex="ex-other",
        s=0.9,
        n_created=N_BASE,
    )
    create_edge(gremlin, SPACE, space_id=SPACE, from_key="hot", to_key="other", label="temporal")
    index_vector(
        es,
        space_id=SPACE,
        node_key="hot",
        vector=[1.0, 0.0, 0.0, 0.0],
        content="m5 integration memory",
    )
    index_vector(
        es, space_id=SPACE, node_key="other", vector=[0.9, 0.44, 0.0, 0.0], content="neighbor note"
    )
    redis.set(f"ex:n:{SPACE}", N_BASE)

    ctx = ApiContext(
        gremlin=gremlin, es=es, ex_session=ex_session, redis=redis, meta_appender=lambda **kw: None
    )
    ctx.meta_appender = _make_background_appender(ctx)
    app = create_app(ctx)
    with _serve(app) as http:
        yield SimpleNamespace(
            ctx=ctx, http=http, redis=redis, ex_session=ex_session, gremlin=gremlin, app=app
        )
    gremlin.submit("ConfiguredGraphFactory.close(gname); 'closed'", {"gname": SPACE}).all().result()
    ex_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name(SPACE)}")  # EX 测试数据，可 DROP
    gremlin.close()
    es.close()
    redis.delete(f"ex:n:{SPACE}")


@contextlib.contextmanager
def _serve(app):
    """在空闲端口起真实 uvicorn（daemon 线程），产出同步 httpx.Client。

    httpx.ASGITransport 只支持 AsyncClient；走真实 HTTP 让 SDK（同步 httpx.Client）
    与原生调用共用一条路径，测试更贴近部署形态。
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.1)
    assert server.started, "uvicorn 启动超时"
    http = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10.0)
    try:
        yield http
    finally:
        http.close()
        server.should_exit = True
        thread.join(timeout=5)


def _post(stack, op, payload, token=FULL_TOKEN):
    return stack.http.post(
        f"/memory/{op}", json=payload, headers={"Authorization": f"Bearer {token}"}
    )


def _ex_rows(stack, table="experience_events"):
    return list(stack.ex_session.execute(f"SELECT * FROM {keyspace_name(SPACE)}.{table}"))


# ------------------------------------------- 验收 1：record/flag_conflict 等 EX ack


def test_record_returns_after_ex_ack(stack):
    resp = _post(stack, "record", {"space_id": SPACE, "content": "first event"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["n"] == N_BASE + 1
    # ack 语义：返回后事件必须已可在 EX 查到（不是提交即返回）
    rows = [r for r in _ex_rows(stack) if r.n == N_BASE + 1]
    assert len(rows) == 1
    assert rows[0].content == "first event"
    assert rows[0].agent_actor_id == "claude-code"  # 盖章来自 claim


def test_second_record_increments_n(stack):
    resp = _post(stack, "record", {"space_id": SPACE, "content": "second event"})
    assert resp.json()["n"] == N_BASE + 2


def test_flag_conflict_stored_with_ref(stack):
    resp = _post(
        stack,
        "flag_conflict",
        {"space_id": SPACE, "content": "corrected fact", "ref_conflict": "hot"},
    )
    assert resp.status_code == 200
    rows = [r for r in _ex_rows(stack) if r.ref_conflict == "hot"]
    assert len(rows) == 1 and rows[0].content == "corrected fact"


# ------------------------------------------- 验收 2：agent_actor_id 伪造


def test_actor_spoof_rejected(stack):
    before = len(_ex_rows(stack))
    resp = _post(
        stack, "record", {"space_id": SPACE, "content": "forged", "agent_actor_id": "evil-twin"}
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "actor_spoof"
    assert len(_ex_rows(stack)) == before  # 伪造请求未产生任何 EX 记录


# ---------------------------------------------------------------- 鉴权矩阵


def test_scope_forbidden(stack):
    token = _token(("retrieve",))
    resp = _post(stack, "record", {"space_id": SPACE, "content": "x"}, token=token)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden_scope"


def test_space_forbidden(stack):
    token = _token(("record",), space_ids=("other_space",))
    resp = _post(stack, "record", {"space_id": SPACE, "content": "x"}, token=token)
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "forbidden_space"


def test_expired_token(stack):
    token = _token(("record",), exp=int(time.time()) - 10)
    resp = _post(stack, "record", {"space_id": SPACE, "content": "x"}, token=token)
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


def test_bad_signature(stack):
    token = _token(("record",), secret="wrong")
    resp = _post(stack, "record", {"space_id": SPACE, "content": "x"}, token=token)
    assert resp.status_code == 401


# ------------------------------------------- 验收 3：debug scope 裁剪（字段级）


def test_retrieve_default_crops_ff_fields(stack):
    resp = _post(stack, "retrieve", {"space_id": SPACE, "query_vector": [1.0, 0.0, 0.0, 0.0]})
    assert resp.status_code == 200
    nodes = resp.json()["nodes"]
    assert {n["node_key"] for n in nodes} >= {"hot"}
    for node in nodes:
        assert "s_effective" not in node
        assert "phi" not in node
        assert set(node) == {"node_key", "content", "tau", "brief"}
    assert any(e["label"] == "temporal" for e in resp.json()["edges"])  # 带边子图


def test_retrieve_debug_scope_exposes_phi(stack):
    resp = _post(
        stack,
        "retrieve",
        {"space_id": SPACE, "query_vector": [1.0, 0.0, 0.0, 0.0]},
        token=DEBUG_TOKEN,
    )
    assert resp.status_code == 200
    hot = next(n for n in resp.json()["nodes"] if n["node_key"] == "hot")
    assert hot["s_effective"] is not None
    assert hot["phi"]["n_star_cached"] > 0
    assert "reinforce_count" in hot["phi"]


# ------------------------------------------- 验收 4：reinforce 直连 + 异步元事件


def test_reinforce_sync_effect_and_async_meta(stack):
    n_before = int(stack.redis.get(f"ex:n:{SPACE}"))
    resp = _post(stack, "reinforce", {"space_id": SPACE, "node_key": "hot"})
    assert resp.status_code == 200
    assert resp.json() == {"node_key": "hot", "applied": True}  # 无 debug scope 不含 φ

    # 同步生效：RMS 图侧 s 立即 +0.2（0.5 → 0.7）
    phi = read_phi(stack.gremlin, SPACE, space_id=SPACE, node_key="hot")
    assert phi.s == pytest.approx(0.7)
    assert phi.reinforce_count == 1

    # 异步元事件：fire-and-forget 追加，轮询等待到达
    deadline = time.time() + 10
    rows = []
    while time.time() < deadline:
        rows = [
            r
            for r in _ex_rows(stack, "meta_events")
            if r.node_key == "hot" and r.meta_type == "reinforce"
        ]
        if rows:
            break
        time.sleep(0.25)
    assert len(rows) == 1
    assert rows[0].count == 1  # 时间窗合并归 M7，M5 每笔一笔
    assert rows[0].agent_actor_id == "claude-code"

    # 元事件不推进 n（"用得越多忘得越快"防线）
    assert int(stack.redis.get(f"ex:n:{SPACE}")) == n_before


def test_reinforce_has_no_consolidation_path():
    """结构性验收：service 层 import 依赖中不存在 Pulsar/consolidation（零调用的前提是零入口）。"""
    import ast
    import inspect as _inspect

    import lethefield_api.service as svc

    tree = ast.parse(_inspect.getsource(svc))
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    assert not any("pulsar" in m or "consolidation" in m for m in modules), modules


# ------------------------------------------- 验收 5：限流中间件挂载点


def test_rate_limiter_mount_point(stack):
    class DenyAll:
        def allow(self, claims, operation) -> bool:
            return False

    app = create_app(stack.ctx, rate_limiter=DenyAll())
    with _serve(app) as http:
        resp = http.post(
            "/memory/record",
            json={"space_id": SPACE, "content": "x"},
            headers={"Authorization": f"Bearer {FULL_TOKEN}"},
        )
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limited"


# ------------------------------------------- SDK 四接口


def test_sdk_roundtrip(stack):
    sdk = MemoryClient(stack.http.base_url, FULL_TOKEN)
    rec = sdk.record(space_id=SPACE, content="sdk event")
    assert rec["n"] > 0
    assert sdk.flag_conflict(space_id=SPACE, content="sdk fix", ref_conflict="other")
    assert sdk.reinforce(space_id=SPACE, node_key="other")["applied"] is True
    result = sdk.retrieve(space_id=SPACE, query_vector=[1.0, 0.0, 0.0, 0.0])
    assert {n["node_key"] for n in result["nodes"]} >= {"hot"}

    with pytest.raises(ApiError) as exc:
        sdk._call("record", {"space_id": SPACE, "content": "y", "agent_actor_id": "spoof"})
    assert exc.value.code == "actor_spoof"
    sdk.close()
