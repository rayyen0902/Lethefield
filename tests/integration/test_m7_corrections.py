"""M7 纠错机制（supersedes）验收的集成测试（开发文档 §8 五条验收标准）。

真实组件：JanusGraph（热图）、cassandra-ex（EX 事件流）、cassandra-cell
（archived_nodes 冷存表）、ES（rms_vectors）、Redis（n 计数）。

每条用例独立 space（gname = space_id 约定），函数级夹具；图 m7_* 按红线 5
只 close 不 DROP；EX keyspace 是测试数据，用完 DROP。
"""

import subprocess
import sys
import uuid
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL
from lethefield_api import ex_ingest, service
from lethefield_api.auth import Claims
from lethefield_clients import (
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    list_archived,
    list_meta_events,
    redis_client,
)
from lethefield_rms import corrections, ff, rebuild, writer
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
    """独立 space：建图 + EX keyspace（含两表）；用毕 close 图、DROP EX、清 Redis。"""
    gname = f"m7_{uuid.uuid4().hex[:8]}"
    ensure_graph_schema(stack.client, gname)
    ex_ingest.ensure_ex_keyspace(stack.ex_session, gname)
    yield gname
    for name in (gname, f"{gname}_rebuilt"):
        stack.client.submit(
            "try { ConfiguredGraphFactory.close(gname) } catch (ignored) {}; 'closed'",
            {"gname": name},
        ).all().result()
    stack.ex_session.execute(f"DROP KEYSPACE IF EXISTS ex_{gname}")
    stack.redis.delete(f"ex:n:{gname}")


def _ingest(stack, space, content, *, ref_conflict=None):
    """写一条 EX 经验事件（生产摄入路径），返回 (event_id, n)。"""
    return ex_ingest.append_experience(
        stack.ex_session,
        stack.redis,
        space_id=space,
        content=content,
        agent_actor_id="m7-actor",
        account_id="m7-account",
        tau_ms=TAU,
        ref_conflict=ref_conflict,
    )


def _write_node(stack, gname, node_key, event_id, *, s=1.0, n_created=1, content="m7 node"):
    """模拟 M15 入链：EX 事件落图为 event 顶点（ref_ex 关联回 EX）。"""
    writer.create_event_node(
        stack.client,
        gname,
        node_key=node_key,
        space_id=gname,
        content=content,
        tau_ms=TAU,
        ref_ex=event_id,
        s=s,
        n_created=n_created,
    )


def _edge_exists(stack, gname, from_key, to_key, label, *, sid=None) -> bool:
    result = (
        stack.client.submit(
            "def t = ConfiguredGraphFactory.open(gname).traversal(); "
            "t.V().has('space_id', sid).has('node_key', fk)"
            ".outE(lbl).where(__.inV().has('node_key', tk)).hasNext()",
            {
                "gname": gname,
                "sid": sid or gname,  # 重建图：顶点 space_id = 源 space，图名不同
                "fk": from_key,
                "tk": to_key,
                "lbl": label,
            },
        )
        .all()
        .result()
    )
    return bool(result and result[0])


def _n_now(stack, space) -> int:
    return ex_ingest.n_now(stack.redis, stack.ex_session, space_id=space)


# ------------------------------------------------------- 验收 1：schema 无失效标志字段


def test_schema_has_no_invalidation_flags(space):
    """巡检脚本真实执行：图 schema 不存在任何'硬失效标志'字段（M7 红线）。"""
    proc = subprocess.run(
        [sys.executable, "scripts/check_rms_schema.py", "--graph", space],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr


# --------------------------------- 验收 2：纠错 → supersedes 边 + −0.5；同一对幂等；pending 语义


def test_flag_conflict_processed_idempotently(stack, space):
    e1_id, _ = _ingest(stack, space, "fact v1")
    _write_node(stack, space, "old", e1_id, s=1.0, n_created=1)
    e2_id, n2 = _ingest(stack, space, "fact v2", ref_conflict="old")
    _write_node(stack, space, "new", e2_id, s=1.0, n_created=n2)

    stats = corrections.process_corrections(
        stack.client, stack.ex_session, gname=space, space_id=space, n_now=_n_now(stack, space)
    )
    assert (stats.applied, stats.duplicate, stats.pending) == (1, 0, 0)
    assert _edge_exists(stack, space, "new", "old", "supersedes")

    phi = ff.read_phi(stack.client, space, space_id=space, node_key="old")
    assert phi.s == pytest.approx(0.5)  # 1.0 − 0.5
    assert phi.conflict_count == 1
    assert phi.n_last_touched == 2  # conflict δ 更新 n_last_touched

    # 纠错事件尚无对应图节点 → pending（EX 不可变记录保证下轮再处理）
    _ingest(stack, space, "fact v3", ref_conflict="old")
    stats = corrections.process_corrections(
        stack.client, stack.ex_session, gname=space, space_id=space, n_now=_n_now(stack, space)
    )
    assert (stats.applied, stats.duplicate, stats.pending) == (0, 1, 1)
    phi_after = ff.read_phi(stack.client, space, space_id=space, node_key="old")
    assert phi_after.s == pytest.approx(0.5)  # 重复纠错不重复扣分
    assert phi_after.conflict_count == 1
    # supersedes 边仍只有一条
    assert _edge_exists(stack, space, "new", "old", "supersedes")


# --------------------------------- 验收 3：链式纠错 A→B→C——默认返回 C，trace_history 追溯 A/B


def test_chain_correction_retrieval(stack, space):
    e_a, _ = _ingest(stack, space, "version A of the fact")
    _write_node(stack, space, "A", e_a, n_created=1, content="version A of the fact")
    e_b, n_b = _ingest(stack, space, "version B of the fact", ref_conflict="A")
    _write_node(stack, space, "B", e_b, n_created=n_b, content="version B of the fact")
    e_c, n_c = _ingest(stack, space, "version C of the fact", ref_conflict="B")
    _write_node(stack, space, "C", e_c, n_created=n_c, content="version C of the fact")

    stats = corrections.process_corrections(
        stack.client, stack.ex_session, gname=space, space_id=space, n_now=_n_now(stack, space)
    )
    assert stats.applied == 2  # B→A、C→B 两条 supersedes 边

    vecs = {"A": [1.0, 0.0, 0.0, 0.0], "B": [0.9, 0.1, 0.0, 0.0], "C": [0.8, 0.2, 0.0, 0.0]}
    for key, vec in vecs.items():
        index_vector(stack.es, index=VECTORS_INDEX, space_id=space, node_key=key, vector=vec)

    n_now = _n_now(stack, space)
    default = retrieve(
        stack.client,
        stack.es,
        space,
        space_id=space,
        query_vector=[1.0, 0.0, 0.0, 0.0],
        n_now=n_now,
    )
    keys = {n.node_key for n in default.nodes}
    assert "C" in keys  # 默认重定向至链尾最新有效节点
    assert "A" not in keys and "B" not in keys  # 被取代节点不进候选池

    traced = retrieve(
        stack.client,
        stack.es,
        space,
        space_id=space,
        query_vector=[1.0, 0.0, 0.0, 0.0],
        n_now=n_now,
        trace_history=True,  # "当时认为的是什么"类查询：显式追溯历史
    )
    traced_keys = {n.node_key for n in traced.nodes}
    assert {"A", "B", "C"} <= traced_keys
    supersedes = {(e.out_key, e.in_key) for e in traced.edges if e.label == "supersedes"}
    assert ("B", "A") in supersedes and ("C", "B") in supersedes


# ----------------------------- 验收 4：reinforce 时间窗合并——EX 一笔、RMS 每次同步生效


def test_reinforce_window_merge(stack, space):
    e1_id, _ = _ingest(stack, space, "hot fact")
    _write_node(stack, space, "hot", e1_id, s=0.5, n_created=1)

    ctx = service.ApiContext(
        gremlin=stack.client,
        es=stack.es,
        ex_session=stack.ex_session,
        redis=stack.redis,
        meta_appender=lambda **kw: ex_ingest.append_meta(stack.ex_session, **kw),  # 注入同步实现
    )
    claims = Claims(
        account_id="m7-account",
        space_ids=(space,),
        agent_actor_id="m7-actor",
        scopes=("reinforce",),
    )
    for _ in range(3):
        result = service.reinforce(ctx, claims, space_id=space, node_key="hot")
        assert result["applied"] is True

    metas = list_meta_events(stack.ex_session, space_id=space, node_key="hot")
    assert len(metas) == 1  # 窗口内三次强化合并为一笔
    assert metas[0].count == 3
    assert metas[0].meta_type == "reinforce"

    phi = ff.read_phi(stack.client, space, space_id=space, node_key="hot")
    assert phi.reinforce_count == 3  # RMS 侧每次调用均同步生效
    assert phi.s == pytest.approx(1.0)  # 0.5 + 0.6（upper 截断）


# --------------------------------- 验收 5：EX 重放重建——图结构/δ 历史/supersedes/归档表


def test_rebuild_from_ex(stack, space):
    """从 EX 重放重建到新图，逐项比对节点 φ、temporal/supersedes 边与 archived_nodes。

    s 保真走注入 s_resolver（M14 前两档验收的注入点）；理想化 sweep 用大 n_neglect
    隔离忽视干扰，归档判定复用 ff.archive_eligible 重推。
    """
    ff_config = ff.FFConfig(n_neglect=10_000)
    e1_id, _ = _ingest(stack, space, "alpha fact")
    # 窗口外两笔 reinforce 元事件（count=2 覆盖合并展开）
    ex_ingest.append_meta(
        stack.ex_session,
        space_id=space,
        node_key=rebuild.node_key_of(e1_id),
        meta_type="reinforce",
        n_at_event=1,
        agent_actor_id="m7-actor",
        account_id="m7-account",
        count=2,
    )
    e2_id, _ = _ingest(stack, space, "alpha corrected", ref_conflict=rebuild.node_key_of(e1_id))
    e3_id, _ = _ingest(stack, space, "low s fact")
    for _ in range(42):  # 推 n 越过归档视界（e3：s=0.31 → n_star=n+1 → n+41 归档）
        _ingest(stack, space, "filler")

    known_s = {e1_id: 0.6, e2_id: 1.0, e3_id: 0.31}
    target = f"{space}_rebuilt"
    plan = rebuild.rebuild_space(
        stack.client,
        stack.cell_session,
        stack.ex_session,
        space_id=space,
        target_gname=target,
        s_resolver=lambda e: known_s.get(e.event_id, 1.0),
        ff_config=ff_config,
    )

    k1, k2, k3 = (rebuild.node_key_of(e) for e in (e1_id, e2_id, e3_id))
    # 图结构：temporal 边 n 序链、supersedes 边 k2→k1（重建图顶点 space_id = 源 space）
    assert _edge_exists(stack, target, k2, k1, "supersedes", sid=space)
    assert _edge_exists(stack, target, k1, k2, "temporal", sid=space)
    assert (k2, k1) in plan.supersedes_edges

    # δ 历史：k1 = reinforce×2（0.6+0.4）+ conflict−0.5 → s=0.5，rc=2，cc=1
    phi1 = ff.read_phi(stack.client, target, space_id=space, node_key=k1)
    assert phi1.s == pytest.approx(0.5)
    assert (phi1.reinforce_count, phi1.conflict_count, phi1.neglect_count) == (2, 1, 0)
    assert phi1.n_last_touched == 2  # conflict touch 到 e2 的 n
    planned1 = next(n for n in plan.nodes if n.node_key == k1)
    assert phi1.s == pytest.approx(planned1.s)

    # 归档表：e3 经 archive_eligible 确定性重推归档，快照经 libs/clients 访问层可读
    archived = {row["node_key"]: row for row in list_archived(stack.cell_session, target)}
    assert k3 in archived
    props = archived[k3]["snapshot"]["props"]
    assert props["content"] == "low s fact"
    assert props["ref_ex"] == e3_id
    assert props["s"] == pytest.approx(0.31)
    assert [key for key, _ in plan.archives] == [k3]
