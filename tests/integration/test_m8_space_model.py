"""M8 记忆空间模型与鉴权（space_id）验收的集成测试（开发文档 §9 验收标准）。

真实组件与 M5 相同（cassandra-ex / Redis / JanusGraph / ES + 真实 uvicorn）。

验收覆盖：
1. 同一 space_id 下不同 agent_actor_id 的写入共享同一 EX/RMS，且事件可按
   agent_actor_id 过滤来源（RMS 侧 agent_actor_id 是顶点属性 A_i，非分区维度）。
2. 分区键统一 space_id 的 fail-closed 防线：非法 space_id 一律 400 bad_request，
   零副作用（无 EX 写入、无新 keyspace）。
3. 「无 agent 级分区键残留」「核心服务无空间类型分支」由
   scripts/check_space_model.py 静态巡检覆盖（不起栈，CI 接线）。
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

TEST_SECRET = "m8-integration-secret"
# 不在 import 时设 LETHEFIELD_JWT_SECRET：pytest 先导入全部测试模块再执行，
# import 时设值会覆盖 test_m5_api 的密钥（env 进程级共享）使其 token 全部 401。
# 改在模块 fixture 里设定——本模块执行窗口内 env 必然等于 TEST_SECRET。

from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL  # noqa: E402
from lethefield_api.ex_ingest import ensure_ex_keyspace, keyspace_name  # noqa: E402
from lethefield_api.http_app import create_app  # noqa: E402
from lethefield_api.service import ApiContext, _make_background_appender  # noqa: E402
from lethefield_clients import (  # noqa: E402
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceMapping,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    redis_client,
)
from lethefield_clients.ex_n import list_experience_events  # noqa: E402
from lethefield_rms.schema import ensure_graph_schema  # noqa: E402
from lethefield_rms.writer import create_event_node  # noqa: E402

SPACE = f"m8_{uuid.uuid4().hex[:8]}"  # [a-z0-9_] 命名约束（M8 定案）
TAU = 1_720_000_000_000


def _token(scopes, space_ids=(SPACE,), actor="claude-code", account="acct-m8"):
    return jwt.encode(
        {
            "account_id": account,
            "space_id": list(space_ids),
            "agent_actor_id": actor,
            "scope": list(scopes),
            "exp": int(time.time()) + 600,
        },
        TEST_SECRET,
        algorithm="HS256",
    )


# 同一 space 的两个写入者身份（开发者场景：Claude Code / Codex 同空间协作）
TOKEN_A = _token(("record", "retrieve"), actor="claude-code")
TOKEN_B = _token(("record", "retrieve"), actor="codex")


@pytest.fixture(scope="module")
def stack():
    os.environ["LETHEFIELD_JWT_SECRET"] = TEST_SECRET  # 见文件头注释：须在本模块执行窗口内设定
    gremlin = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(gremlin, SPACE)
    ex_session = ex_cassandra_cluster().connect()
    ensure_ex_keyspace(ex_session, SPACE)
    redis = redis_client()
    es = es_client(ES_GRAPH_URL)

    # M9：四操作经映射缓存解析——夹具保留直接建存储，补"先存储后注册"的注册步
    cell_cluster = cassandra_cluster()
    store = MappingTableControlPlaneStore(cell_cluster.connect())
    store.ensure_tables()
    store.register_space(
        SpaceMapping(
            space_id=SPACE,
            cell_id="cell-local",
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
        )
    )

    ctx = ApiContext(
        gremlin=gremlin,
        es=es,
        ex_session=ex_session,
        redis=redis,
        meta_appender=lambda **kw: None,
        mapping_cache=MappingCache(store),
    )
    ctx.meta_appender = _make_background_appender(ctx)
    app = create_app(ctx)
    with _serve(app) as http:
        yield SimpleNamespace(
            ctx=ctx, http=http, redis=redis, ex_session=ex_session, gremlin=gremlin
        )
    gremlin.submit("ConfiguredGraphFactory.close(gname); 'closed'", {"gname": SPACE}).all().result()
    ex_session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name(SPACE)}")  # EX 测试数据，可 DROP
    store.unregister_space(SPACE)
    cell_cluster.shutdown()
    gremlin.close()
    es.close()
    redis.delete(f"ex:n:{SPACE}")


@contextlib.contextmanager
def _serve(app):
    """在空闲端口起真实 uvicorn（daemon 线程），与 M5 同形态。"""
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


def _post(stack, op, payload, token):
    return stack.http.post(
        f"/memory/{op}", json=payload, headers={"Authorization": f"Bearer {token}"}
    )


# ------------------------------------------- 验收 1：多写入者共享同一 EX/RMS


def test_multi_writer_share_ex(stack):
    """同 space 两个 agent_actor_id 各 record 一条：同 keyspace、n 连续、可按来源过滤。"""
    resp_a = _post(stack, "record", {"space_id": SPACE, "content": "note from claude"}, TOKEN_A)
    resp_b = _post(stack, "record", {"space_id": SPACE, "content": "note from codex"}, TOKEN_B)
    assert resp_a.status_code == 200 and resp_b.status_code == 200
    # n 空间级单调：两个写入者推进同一条 n 序列
    assert resp_a.json()["n"] == 1
    assert resp_b.json()["n"] == 2

    events = list_experience_events(stack.ex_session, space_id=SPACE)
    assert [e.content for e in events] == ["note from claude", "note from codex"]
    # 按 agent_actor_id 过滤来源（节点属性维度，不是分区维度）
    by_actor = {}
    for e in events:
        by_actor.setdefault(e.agent_actor_id, []).append(e.content)
    assert by_actor == {"claude-code": ["note from claude"], "codex": ["note from codex"]}
    # 两写入者同属一个账号层级
    assert {e.account_id for e in events} == {"acct-m8"}


def test_multi_writer_share_rms(stack):
    """RMS 侧：两写入者的节点落在同一图（图名 = space_id），agent_actor_id 属性各自正确。"""
    create_event_node(
        stack.gremlin,
        SPACE,
        node_key="nk_claude",
        space_id=SPACE,
        content="note from claude",
        tau_ms=TAU,
        ref_ex="ex-claude",
        s=0.8,
        n_created=1,
        agent_actor_id="claude-code",
    )
    create_event_node(
        stack.gremlin,
        SPACE,
        node_key="nk_codex",
        space_id=SPACE,
        content="note from codex",
        tau_ms=TAU,
        ref_ex="ex-codex",
        s=0.8,
        n_created=2,
        agent_actor_id="codex",
    )
    rows = (
        stack.gremlin.submit(
            "ConfiguredGraphFactory.open(gname).traversal().V()"
            ".has('space_id', sid).has('node_type', 'event')"
            ".valueMap('node_key', 'agent_actor_id').toList()",
            {"gname": SPACE, "sid": SPACE},
        )
        .all()
        .result()
    )
    got = {r["node_key"][0]: r["agent_actor_id"][0] for r in rows}
    assert got == {"nk_claude": "claude-code", "nk_codex": "codex"}

    # 来源过滤：按 agent_actor_id 只看到对应写入者的节点
    codex_only = (
        stack.gremlin.submit(
            "ConfiguredGraphFactory.open(gname).traversal().V()"
            ".has('space_id', sid).has('agent_actor_id', 'codex')"
            ".values('node_key').toList()",
            {"gname": SPACE, "sid": SPACE},
        )
        .all()
        .result()
    )
    assert codex_only == ["nk_codex"]


# ------------------------------------------- 验收 2：非法 space_id fail-closed


@pytest.mark.parametrize("bad", ["Bad_Space", "has-dash", "x" * 41])
def test_invalid_space_id_rejected(stack, bad):
    """非法 space_id：record/retrieve 均 400 bad_request，零副作用。"""
    keyspaces_before = {
        r.keyspace_name
        for r in stack.ex_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    }
    ex_rows_before = len(list_experience_events(stack.ex_session, space_id=SPACE))

    token = _token(("record", "retrieve"), space_ids=(bad,))  # 凭证覆盖但名字非法
    resp = _post(stack, "record", {"space_id": bad, "content": "x"}, token)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "bad_request"
    resp = _post(stack, "retrieve", {"space_id": bad, "query_text": "x"}, token)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "bad_request"

    # 零副作用：无新 keyspace、合法 space 无新增事件
    keyspaces_after = {
        r.keyspace_name
        for r in stack.ex_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    }
    assert keyspaces_after == keyspaces_before
    assert len(list_experience_events(stack.ex_session, space_id=SPACE)) == ex_rows_before
