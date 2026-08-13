"""M16 IS 简版验收的集成测试（开发文档 §17 四条验收标准 + v1.2 修订记录第 24 条）。

真实组件：全栈（cassandra-cell/ex、JanusGraph、ES、Redis、Pulsar admin、Postgres）。
空间创建走 IS 入口 → M9/M10 真实 provision 流水线；HTTP 层起真实 uvicorn（同 M5）。

验收映射：
1. 账号 → 空间 → 写入者凭证三级关系完整，claim 字段与契约 3 一致（+jti/exp/iat 演进）；
2. 空间创建触发真实开通流水线；provision 中途失败 → 无映射、无归属行（无半开通状态）；
3. 吊销后的凭证在受保护接口上被 401 拒绝（吊销列表立即生效）；
4. C 端申请 debug scope 拒签；internal 签发 debug 凭证可在 retrieve 取回 φ 内部字段
   （与 M5 验收项联动）。附带：授权注册表入口在 IS 侧（§12.4）。
"""

import contextlib
import os
import socket
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import httpx
import jwt
import pytest
import uvicorn
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_api.http_app import create_app
from lethefield_api.service import ApiContext
from lethefield_clients import (
    AuthRegistryStore,
    AuthScope,
    CredentialStore,
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pg_connection,
    redis_client,
    space_ref_of,
)
from lethefield_is import tokens
from lethefield_is.__main__ import main as is_cli
from lethefield_is.service import create_space
from lethefield_is.store import IsStore
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector
from lethefield_rms.writer import create_edge, create_event_node
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.provision import ProvisionDeps, provision_space

SECRET = "m16-integration-secret"
ACCOUNT = f"acct_m16_{uuid.uuid4().hex[:6]}"
SPACE = f"m16_{uuid.uuid4().hex[:8]}"
N_BASE = 100
TAU = 1_720_000_000_000

_MIGRATION = Path(__file__).parents[2] / "deploy/postgres/migrations/002_is.sql"


def _apply_is_migration() -> None:
    """幂等应用 IS 三表（CI 新卷 init.sql 已含；既有 dev 卷靠本步兜底）。"""
    with pg_connection() as conn, conn.cursor() as cur:
        for stmt in _MIGRATION.read_text().split(";"):
            if stmt.strip():
                cur.execute(stmt)


@pytest.fixture(scope="module")
def stack():
    # JWT 密钥在模块 fixture 内设定（M8/M14 教训：import 时设定会被后导入模块覆盖）
    old_secret = os.environ.get("LETHEFIELD_JWT_SECRET")
    os.environ["LETHEFIELD_JWT_SECRET"] = SECRET
    wait_for_gremlin()
    _apply_is_migration()

    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    cell_session = cell_cluster.connect()
    store = MappingTableControlPlaneStore(cell_session)
    store.ensure_tables()
    try:
        store.get_cell(local_cell().cell_id)
    except KeyError:
        store.register_cell(local_cell())
    gremlin = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)
    redis = redis_client()

    is_store = IsStore()
    cred_store = CredentialStore()
    is_store.create_account(ACCOUNT, "m16 集成测试账号")
    deps = ProvisionDeps(
        store=store, gremlin=gremlin, ex_session=ex_cluster.connect(), cell_session=cell_session
    )
    # IS 空间创建入口 → 真实 M9/M10 开通流水线（验收 2 的放行侧）
    mapping = create_space(
        is_store,
        lambda space_id, tier: provision_space(deps, space_id, tier=tier),
        account_id=ACCOUNT,
        space_id=SPACE,
    )
    assert mapping.space_id == SPACE

    # 检索数据面（同 M5 夹具形态）
    create_event_node(
        gremlin,
        SPACE,
        node_key="hot",
        space_id=SPACE,
        content="m16 integration memory",
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
        content="m16 integration memory",
    )
    index_vector(
        es, space_id=SPACE, node_key="other", vector=[0.9, 0.44, 0.0, 0.0], content="neighbor note"
    )
    redis.set(f"ex:n:{SPACE}", N_BASE)

    ctx = ApiContext(
        gremlin=gremlin,
        es=es,
        ex_session=ex_cluster.connect(),
        redis=redis,
        meta_appender=lambda **kw: None,
        mapping_cache=MappingCache(store),
    )
    # 默认吊销检查 = 真实 CredentialStore（PG）——验收 3 走生产同形态
    app = create_app(ctx)
    with _serve(app) as http:
        yield SimpleNamespace(
            http=http,
            store=store,
            is_store=is_store,
            cred_store=cred_store,
            gremlin=gremlin,
            es=es,
            redis=redis,
            cell_session=cell_session,
            ex_cluster=ex_cluster,
        )

    # 清理：destroy 真实流水线（广播注入空操作，契约 5 通道归 M9/M10 用例）
    destroy_space(
        DestroyDeps(
            store=store,
            gremlin=gremlin,
            cell_session=cell_session,
            ex_session=ex_cluster.connect(),
            es=es,
        ),
        SPACE,
        broadcast_destroy=lambda space_id: None,
    )
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM is_credentials WHERE account_id = %s", (ACCOUNT,))
        cur.execute("DELETE FROM is_space_owners WHERE account_id = %s", (ACCOUNT,))
        cur.execute("DELETE FROM is_accounts WHERE account_id = %s", (ACCOUNT,))
        cur.execute("DELETE FROM auth_registry WHERE space_ref = %s", (space_ref_of(SPACE),))
    redis.delete(f"ex:n:{SPACE}")
    gremlin.close()
    es.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()
    if old_secret is None:
        del os.environ["LETHEFIELD_JWT_SECRET"]
    else:
        os.environ["LETHEFIELD_JWT_SECRET"] = old_secret


@contextlib.contextmanager
def _serve(app):
    """在空闲端口起真实 uvicorn（daemon 线程），产出同步 httpx.Client（同 M5 形态）。"""
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


def _issue(stack, scopes, internal=False):
    return tokens.issue_token(
        stack.cred_store,
        account_id=ACCOUNT,
        space_ids=[SPACE],
        agent_actor_id="writer-m16",
        scopes=scopes,
        internal=internal,
    )


def _retrieve(stack, token):
    return stack.http.post(
        "/memory/retrieve",
        json={"space_id": SPACE, "query_vector": [1.0, 0.0, 0.0, 0.0]},
        headers={"Authorization": f"Bearer {token}"},
    )


# ------------------------------------------- 验收 1：账号 → 空间 → 凭证三级关系


def test_three_level_relation_and_claim_structure(stack):
    account = stack.is_store.get_account(ACCOUNT)
    assert account is not None and account.status == "active"
    assert stack.is_store.list_spaces_of(ACCOUNT) == [SPACE]
    assert stack.store.get_space_mapping(SPACE).space_id == SPACE  # 映射已注册

    token = _issue(stack, ["record", "retrieve"])
    payload = jwt.decode(token, SECRET, algorithms=["HS256"])
    # 契约 3 四字段 + 首次演进的 jti/exp/iat（修订记录 24①）
    assert {"account_id", "space_id", "agent_actor_id", "scope", "jti", "exp", "iat"} <= set(
        payload
    )
    assert payload["account_id"] == ACCOUNT
    assert payload["space_id"] == [SPACE]
    assert payload["agent_actor_id"] == "writer-m16"
    assert payload["scope"] == ["record", "retrieve"]

    record = stack.cred_store.get(payload["jti"])
    assert record is not None
    assert record.account_id == ACCOUNT and record.status == "active"
    assert record.internal is False


# ------------------------------------------- 验收 3：签发可用 + 吊销即拒


def test_issued_token_accepted_then_revoked_rejected(stack):
    token = _issue(stack, ["retrieve"])
    resp = _retrieve(stack, token)
    assert resp.status_code == 200
    assert {n["node_key"] for n in resp.json()["nodes"]} >= {"hot"}

    jti = jwt.decode(token, SECRET, algorithms=["HS256"])["jti"]
    assert stack.cred_store.revoke(jti)
    resp = _retrieve(stack, token)
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


# ------------------------------------------- 验收 4：debug scope 签发侧闸门


def test_c_end_debug_scope_rejected(stack):
    with pytest.raises(ValueError, match="debug scope 仅内部签发"):
        _issue(stack, ["retrieve", "debug"], internal=False)


def test_internal_debug_token_exposes_phi(stack):
    token = _issue(stack, ["retrieve", "debug"], internal=True)
    resp = _retrieve(stack, token)
    assert resp.status_code == 200
    hot = next(n for n in resp.json()["nodes"] if n["node_key"] == "hot")
    assert hot["s_effective"] is not None
    assert hot["phi"]["n_star_cached"] > 0
    assert "reinforce_count" in hot["phi"]

    # 对照：同链路非 debug 凭证取不回 FF 内部字段（M5 字段级裁剪联动）
    resp = _retrieve(stack, _issue(stack, ["retrieve"]))
    for node in resp.json()["nodes"]:
        assert "phi" not in node and "s_effective" not in node


# ------------------------------------------- 验收 2：provision 失败无半开通状态


def test_provision_failure_rolls_back_without_ownership(stack, monkeypatch):
    import lethefield_scheduler.provision as provision_mod

    account2 = f"acct_m16rb_{uuid.uuid4().hex[:6]}"
    space2 = f"m16rb_{uuid.uuid4().hex[:8]}"
    stack.is_store.create_account(account2)

    def _boom(*args, **kwargs):
        raise RuntimeError("pulsar admin down")

    deps = ProvisionDeps(
        store=stack.store,
        gremlin=stack.gremlin,
        ex_session=stack.ex_cluster.connect(),
        cell_session=stack.cell_session,
    )
    monkeypatch.setattr(provision_mod.pulsar_admin, "ensure_namespace", _boom)
    with pytest.raises(Exception, match="pulsar admin down"):
        create_space(
            stack.is_store,
            lambda space_id, tier: provision_space(deps, space_id, tier=tier),
            account_id=account2,
            space_id=space2,
        )
    with pytest.raises(SpaceNotFoundError):
        stack.store.get_space_mapping(space2)
    assert stack.is_store.list_spaces_of(account2) == []

    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM is_accounts WHERE account_id = %s", (account2,))


# ------------------------------------------- §12.4：授权注册表入口在 IS 侧


def test_auth_registry_entry_via_is_cli(stack):
    assert is_cli(["auth", "grant", "--space", SPACE, "--scopes", "calibration"]) == 0
    registry = AuthRegistryStore()
    assert registry.is_authorized(space_ref_of(SPACE), AuthScope.CALIBRATION)
    assert not registry.is_authorized(space_ref_of(SPACE), AuthScope.CONTENT_COPY)

    assert is_cli(["auth", "revoke", "--space", SPACE]) == 0
    assert not registry.is_authorized(space_ref_of(SPACE), AuthScope.CALIBRATION)
