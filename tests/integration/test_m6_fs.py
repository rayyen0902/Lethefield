"""M6 FS sweep worker 验收的集成测试（开发文档 §7 五条验收标准）。

真实组件：JanusGraph（热图）、cassandra-cell（archived_nodes 冷存表）、
cassandra-ex（n_now）、ES（rms_vectors 清理验证）、Redis（n 计数 / 心跳）。

每条用例独立 space（gname = space_id 约定），函数级夹具——
sweep 处理整 space 全部节点，共享图会让跨用例节点互相触发流程。
图 m6_* 按红线 5 只 close 不 DROP；EX keyspace 是测试数据，用完 DROP。
"""

import subprocess
import sys
import time
import uuid
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL
from lethefield_clients import (
    SpaceMapping,
    StaticControlPlaneStore,
    cassandra_cluster,
    ensure_archive_table,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    list_archived,
    redis_client,
)
from lethefield_fs.config import HEARTBEAT_KEY
from lethefield_fs.liveness import check_liveness
from lethefield_fs.sweep import sweep_space
from lethefield_fs.worker import run_once
from lethefield_rms import ff
from lethefield_rms.retrieve import retrieve
from lethefield_rms.schema import ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector

TAU = 1_720_000_000_000


@pytest.fixture(scope="module")
def stack():
    client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ex_cluster = ex_cassandra_cluster()
    cell_cluster = cassandra_cluster()
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)
    redis = redis_client()
    yield SimpleNamespace(
        client=client,
        ex_session=ex_cluster.connect(),
        cell_session=cell_cluster.connect(),
        es=es,
        redis=redis,
    )
    client.close()
    ex_cluster.shutdown()
    cell_cluster.shutdown()
    es.close()


@pytest.fixture
def space(stack):
    """独立 space：建图 + EX keyspace（含两表），返回 gname；用毕 close 图、DROP EX。"""
    from lethefield_api.ex_ingest import ensure_ex_keyspace

    gname = f"m6_{uuid.uuid4().hex[:8]}"
    ensure_graph_schema(stack.client, gname)
    ensure_ex_keyspace(stack.ex_session, gname)
    yield gname
    stack.client.submit(
        "ConfiguredGraphFactory.close(gname); 'closed'", {"gname": gname}
    ).all().result()
    stack.ex_session.execute(f"DROP KEYSPACE IF EXISTS ex_{gname}")
    stack.redis.delete(f"ex:n:{gname}", f"{HEARTBEAT_KEY}:{gname}")


def _make_node(stack, gname, node_key, *, s, n, nstar, rc=0, cc=0, nc=0, content="m6 node"):
    """直接造指定 φ 状态的事件节点（手写图数据必须显式给正确视界，M4 踩坑）。"""
    result = (
        stack.client.submit(
            """
        def t = ConfiguredGraphFactory.open(gname).traversal()
        t.addV()
            .property('node_key', nk).property('space_id', sid).property('node_type', 'event')
            .property('content', contentText).property('tau', new Date(tauMs as long))
            .property('ref_ex', refEx)
            .property('s', sVal as double)
            .property('n_created', nCreated as long)
            .property('n_last_touched', nLt as long)
            .property('n_star_cached', nStar as long)
            .property('reinforce_count', rc as int)
            .property('conflict_count', cc as int)
            .property('neglect_count', nc as int)
            .next()
        t.tx().commit()
        'ok'
        """,
            {
                "gname": gname,
                "nk": node_key,
                "sid": gname,
                "contentText": content,
                "tauMs": str(TAU),
                "refEx": f"ex-{node_key}",
                "sVal": s,
                "nCreated": str(n),
                "nLt": str(n),
                "nStar": str(nstar),
                "rc": rc,
                "cc": cc,
                "nc": nc,
            },
        )
        .all()
        .result()
    )
    assert "ok" in result


def _vertex_exists(stack, gname, node_key) -> bool:
    result = (
        stack.client.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal(); "
            "t.V().has('space_id', sid).has('node_key', nk).hasNext()",
            {"gname": gname, "sid": gname, "nk": node_key},
        )
        .all()
        .result()
    )
    return bool(result and result[0])


def _phi(stack, gname, node_key) -> ff.PhiState:
    return ff.read_phi(stack.client, gname, space_id=gname, node_key=node_key)


# ---------------------------------------------------------------- 忽视惩罚（验收 2）


def test_neglect_idempotent_per_interval(stack, space):
    """同一忽视区间重复 sweep 只记一记惩罚；推进 n 跨下一区间后再记一记。"""
    # s=0.9, n*=10 → nstar=10；n_now=20 恰满第一区间
    _make_node(stack, space, "neg", s=0.9, n=0, nstar=10)

    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=20
    )
    assert (stats.neglected, stats.archived, stats.consolidated) == (1, 0, 0)
    phi = _phi(stack, space, "neg")
    assert phi.neglect_count == 1
    assert phi.s == pytest.approx(0.8)
    assert phi.n_last_touched == 0  # 忽视惩罚不更新 n_last_touched（否则自我抵消）

    # 同一区间重跑：不产生重复惩罚
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=20
    )
    assert stats.neglected == 0
    assert _phi(stack, space, "neg").neglect_count == 1

    # 推进到第二区间边界：再记一记
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=40
    )
    assert stats.neglected == 1
    phi = _phi(stack, space, "neg")
    assert phi.neglect_count == 2
    assert phi.s == pytest.approx(0.7)


def test_n_star_refresh_near_horizon(stack, space):
    """临近遗忘视界的节点顺带刷新 n_star_cached（重算值不同才写）。"""
    # s=0.5, n_last_touched=100 → 真实视界 100+ceil(4.606)=105；存入错误值 103
    _make_node(stack, space, "stale", s=0.5, n=100, nstar=103)
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=90
    )
    assert stats.refreshed == 1
    assert _phi(stack, space, "stale").n_star_cached == 105

    # 视界已正确的节点不重复写
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=90
    )
    assert stats.refreshed == 0


# ---------------------------------------------------------------- 归档（验收 3）


def test_archive_end_to_end(stack, space):
    """跨界+宽限期节点：热图移除、archived_nodes 有完整快照（字段+邻接）、ES 向量清理。"""
    _make_node(stack, space, "old", s=0.05, n=0, nstar=0)  # s<θ → n*=0，早已跨界
    _make_node(stack, space, "peer", s=0.9, n=100, nstar=110)
    stack.client.submit(
        "def t = ConfiguredGraphFactory.open(gname).traversal(); "
        "def a = t.V().has('space_id', sid).has('node_key', 'old').next(); "
        "def b = t.V().has('space_id', sid).has('node_key', 'peer').next(); "
        "a.addEdge('temporal', b); t.tx().commit(); 'ok'",
        {"gname": space, "sid": space},
    ).all().result()
    index_vector(stack.es, space_id=space, node_key="old", vector=[1.0, 0.0, 0.0, 0.0])

    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=100
    )
    assert stats.archived == 1

    # 热图移除（顶点连同边消失），peer 不受影响
    assert not _vertex_exists(stack, space, "old")
    assert _vertex_exists(stack, space, "peer")

    # 冷存副本：节点字段 + 图邻接快照
    archived = list_archived(stack.cell_session, space)
    assert [a["node_key"] for a in archived] == ["old"]
    snapshot = archived[0]["snapshot"]
    assert snapshot["props"]["content"] == "m6 node"
    assert snapshot["props"]["ref_ex"] == "ex-old"  # EX 溯源链保留
    assert snapshot["props"]["tau"] == TAU
    assert snapshot["edges"] == [{"label": "temporal", "out_key": "old", "in_key": "peer"}]

    # ES 向量文档已删（防 Stage 2 召回死引用）
    assert not stack.es.exists(index=VECTORS_INDEX, id=f"{space}:old", routing=space)

    # 归档幂等：下一轮 sweep 无可归档节点
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=100
    )
    assert stats.archived == 0


def test_grace_cancelled_by_reinforce(stack, space):
    """定案指定用例：宽限期内一次 reinforce 使归档资格自动取消。"""
    # s=0.31 → n*≈0.3 → nstar=1；n_now=41 恰满 grace_n=40 宽限期
    _make_node(stack, space, "edge", s=0.31, n=0, nstar=1)
    assert ff.archive_eligible(41, 1, ff.DEFAULT_CONFIG.grace_n)  # 前置：资格已成立

    # 宽限期内 reinforce：n_last_touched 前移 → nstar 推过 n_now
    ff.apply_reinforce(stack.client, space, space_id=space, node_key="edge", n_now=41)
    phi = _phi(stack, space, "edge")
    assert phi.n_star_cached > 41

    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=41
    )
    assert stats.archived == 0
    assert _vertex_exists(stack, space, "edge")
    ensure_archive_table(stack.cell_session, space)
    assert list_archived(stack.cell_session, space) == []


# ---------------------------------------------------------------- 固化（验收 3）


def test_consolidate_end_to_end(stack, space):
    """阈值触发固化 → θ 过滤不再生效；固化后 δ 不改 s 但计数器照计；sweep 跳过。"""
    # s=0.05（远低于 θ=0.3）但 reinforce_count 达阈值、无 conflict
    _make_node(stack, space, "solid", s=0.05, n=100, nstar=100, rc=3)
    index_vector(stack.es, space_id=space, node_key="solid", vector=[1.0, 0.0, 0.0, 0.0])

    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=100
    )
    assert (stats.consolidated, stats.archived, stats.neglected) == (1, 0, 0)
    phi = _phi(stack, space, "solid")
    assert phi.consolidated_at is not None
    assert phi.n_star_cached == 2**63 - 1  # LONG_MAX：粗筛放行
    assert phi.s == pytest.approx(0.05)  # s 锁定

    # 固化幂等：重复 sweep 不覆盖首次固化时间戳
    first_ts = phi.consolidated_at
    stats = sweep_space(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, n_now=500
    )
    assert stats.consolidated == 0
    assert stats.skipped_consolidated == 1  # sweep 跳过固化节点（不施 neglect）
    assert _phi(stack, space, "solid").consolidated_at == first_ts

    # 固化后 ±δ 不改 s、计数器照计
    ff.apply_reinforce(stack.client, space, space_id=space, node_key="solid", n_now=500)
    phi = _phi(stack, space, "solid")
    assert phi.s == pytest.approx(0.05)
    assert phi.reinforce_count == 4

    # 检索：低 s 固化节点不被 θ_effective 过滤（s_effective 取 s 现值、跳过衰减）
    result = retrieve(
        stack.client,
        stack.es,
        space,
        space_id=space,
        query_vector=[1.0, 0.0, 0.0, 0.0],
        n_now=500,
    )
    keys = [n.node_key for n in result.nodes]
    assert "solid" in keys
    node = next(n for n in result.nodes if n.node_key == "solid")
    assert node.s_effective == pytest.approx(0.05)


# ---------------------------------------------------------------- n 语义（验收 5）


def test_meta_event_does_not_advance_n(stack, space):
    """元事件（reinforce 追加）不推进 n_now——计数器前后对比（M5 已保证，M6 验收）。"""
    from lethefield_api.ex_ingest import append_meta, n_now

    stack.redis.set(f"ex:n:{space}", 7)
    before = n_now(stack.redis, stack.ex_session, space_id=space)

    append_meta(
        stack.ex_session,
        space_id=space,
        node_key="any",
        meta_type="reinforce",
        n_at_event=before,
        agent_actor_id="test",
        account_id="test",
    )

    assert n_now(stack.redis, stack.ex_session, space_id=space) == before


# ---------------------------------------------------------------- Dead Man's Switch（验收 4）


def test_worker_run_once_and_liveness(stack, space):
    """run_once 写心跳 + 每 space 计数；心跳过期 → liveness 巡检退出码 1。"""
    _make_node(stack, space, "wk", s=0.9, n=0, nstar=10)
    store = StaticControlPlaneStore.local()
    store.register_space(
        SpaceMapping(
            space_id=space,
            cell_id="cell-local",
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
        )
    )

    results = run_once(
        store, stack.client, stack.ex_session, stack.cell_session, stack.es, stack.redis
    )
    assert space in results  # 图存在但无 ex:n 缓存 → n_now 从 EX MAX(n) 重建为 0，无触发

    # 心跳已写：全局 + 每 space
    assert stack.redis.get(HEARTBEAT_KEY) is not None
    assert stack.redis.get(f"{HEARTBEAT_KEY}:{space}") is not None
    assert check_liveness(stack.redis, stale_after_seconds=300) == []

    # 停摆场景：心跳过期 → 巡检告警（退出码 1）；恢复新鲜心跳 → 0
    stack.redis.set(HEARTBEAT_KEY, time.time() - 10000)
    stale = subprocess.run(
        [sys.executable, "-m", "lethefield_fs.liveness", "--stale-after", "300"],
        capture_output=True,
        text=True,
    )
    assert stale.returncode == 1
    assert "心跳停滞" in stale.stdout

    stack.redis.set(HEARTBEAT_KEY, time.time())
    fresh = subprocess.run(
        [sys.executable, "-m", "lethefield_fs.liveness", "--stale-after", "300"],
        capture_output=True,
        text=True,
    )
    assert fresh.returncode == 0
