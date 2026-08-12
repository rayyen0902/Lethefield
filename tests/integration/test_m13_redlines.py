"""M13 多租户工程红线落地的集成测试（开发文档 §14 验收）。

真实组件：JanusGraph（热图）、cassandra-cell（archived_nodes）、cassandra-ex
（EX 事件流）、ES（rms_vectors）、Redis（n 计数）。

覆盖：
- 红线 2 配额拒绝（验收核心）：小 QuotaConfig + 真 QuotaCounters（真图/真 ES 计数）
  注入 writer/vectors，越限写入抛 QuotaExceeded——"压测下触发拒绝"语义。
- 红线 3 归档快照携 v_i：archive_node 删 ES 向量前快照携带原始向量。
- 红线 4/6 运行时汇总核验：scripts/check_redlines.py --runtime 退出码 0。
- 红线 3 重建 v_i 携带：源侧归档快照（带 v）经 EX 重放重建后，目标图 keyspace
  的 archived_nodes 快照仍携带同一 v。

每条用例独立 space（gname = space_id 约定），函数级夹具；图 m13_* 按红线 5
只 close 不 DROP；EX keyspace 是测试数据，用完 DROP。
"""

import subprocess
import sys
import uuid
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL
from lethefield_api import ex_ingest
from lethefield_clients import (
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    list_archived,
    redis_client,
)
from lethefield_fs.archive import archive_node
from lethefield_rms import ff, rebuild, writer
from lethefield_rms.quota import QuotaConfig, QuotaCounters, QuotaExceeded
from lethefield_rms.schema import ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, get_vector, index_vector

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
    """独立 space：建图 + EX keyspace；用毕 close 图（含重建图）、DROP EX、清 Redis/ES。"""
    gname = f"m13_{uuid.uuid4().hex[:8]}"
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
    stack.es.options(ignore_status=(404,)).delete_by_query(
        index=VECTORS_INDEX,
        query={"term": {"space_id": gname}},
        routing=gname,
        conflicts="proceed",
        refresh=True,
    )


# ---------------------------------------------------------------- 红线 2：配额拒绝


def test_quota_rejects_beyond_limits(stack, space):
    """小配额 + 真计数（真图/真 ES）：第 N+1 次写入抛 QuotaExceeded（压测触发拒绝）。

    count_cache_ttl_seconds=0 关闭 TTL 缓存，测试内每次计数都走真图/真 ES——
    生产语义的 TTL 近似执行在 services/rms/tests/test_quota.py 单测覆盖。
    """
    config = QuotaConfig(max_vertices=2, max_edges=0, max_vectors=1, count_cache_ttl_seconds=0.0)
    counters = QuotaCounters(stack.client, stack.es, config)

    def make_node(key: str) -> None:
        writer.create_event_node(
            stack.client,
            space,
            node_key=key,
            space_id=space,
            content="m13 quota node",
            tau_ms=TAU,
            ref_ex=f"ex-{key}",
            s=1.0,
            n_created=1,
            quota=config,
            quota_counters=counters,
        )

    # 顶点配额：上限 2，第 3 次写入拒绝
    make_node("q1")
    make_node("q2")
    with pytest.raises(QuotaExceeded) as excinfo:
        make_node("q3")
    assert excinfo.value.kind == "vertex"
    assert excinfo.value.limit == 2
    assert "quota_exceeded" in str(excinfo.value)

    # 边配额：上限 0，首次建边即拒绝（先查后写，图无副作用）
    with pytest.raises(QuotaExceeded) as excinfo:
        writer.create_edge(
            stack.client,
            space,
            space_id=space,
            from_key="q1",
            to_key="q2",
            label="temporal",
            quota=config,
            quota_counters=counters,
        )
    assert excinfo.value.kind == "edge"

    # 向量配额：上限 1，第 2 条向量写入拒绝
    index_vector(
        stack.es,
        space_id=space,
        node_key="q1",
        vector=[1.0, 0.0, 0.0, 0.0],
        quota=config,
        quota_counters=counters,
    )
    with pytest.raises(QuotaExceeded) as excinfo:
        index_vector(
            stack.es,
            space_id=space,
            node_key="q2",
            vector=[0.0, 1.0, 0.0, 0.0],
            quota=config,
            quota_counters=counters,
        )
    assert excinfo.value.kind == "vector"


# ---------------------------------------------------------------- 红线 3：归档快照携 v_i


def test_archive_snapshot_carries_vector(stack, space):
    """归档独立最小用例：快照携带原始 v_i，rms_vectors 文档同步删除。"""
    vector = [0.25, 0.5, 0.5, 0.5]
    writer.create_event_node(
        stack.client,
        space,
        node_key="arc",
        space_id=space,
        content="m13 archive node",
        tau_ms=TAU,
        ref_ex="ex-arc",
        s=1.0,
        n_created=1,
    )
    index_vector(stack.es, space_id=space, node_key="arc", vector=vector)

    snapshot = archive_node(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, node_key="arc"
    )
    assert snapshot["v"] == pytest.approx(vector)

    # 冷存表读回的快照同样携带 v_i
    archived = list_archived(stack.cell_session, space)
    assert [a["node_key"] for a in archived] == ["arc"]
    assert archived[0]["snapshot"]["v"] == pytest.approx(vector)
    assert archived[0]["snapshot"]["props"]["content"] == "m13 archive node"

    # rms_vectors 文档已删（防 Stage 2 召回死引用）
    assert get_vector(stack.es, space_id=space, node_key="arc") is None


# ---------------------------------------------------------------- 红线 4/6：运行时汇总核验


def test_check_redlines_runtime_passes():
    """check_redlines --runtime（时钟偏移巡检 + 图配置生效值）在全栈上退出码 0。"""
    proc = subprocess.run(
        [sys.executable, "scripts/check_redlines.py", "--runtime"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


# ---------------------------------------------------------------- 红线 3：重建 v_i 携带


def test_rebuild_carries_archived_vector(stack, space):
    """源侧归档快照（带 v）→ EX 重放重建到新图 → 目标 keyspace 归档快照仍携带同一 v。

    链路：EX 事件落图 + rms_vectors → archive_node（源快照带 v、ES 文档已删）→
    rebuild 时 v_i 只能来自源侧旧 archived_nodes 快照（来源①，ES 来源②已无文档）。
    """
    vector = [0.1, 0.2, 0.3, 0.4]
    e1_id, _ = ex_ingest.append_experience(
        stack.ex_session,
        stack.redis,
        space_id=space,
        content="m13 rebuild carry",
        agent_actor_id="m13-actor",
        account_id="m13-account",
        tau_ms=TAU,
    )
    key = rebuild.node_key_of(e1_id)
    writer.create_event_node(
        stack.client,
        space,
        node_key=key,
        space_id=space,
        content="m13 rebuild carry",
        tau_ms=TAU,
        ref_ex=e1_id,
        s=0.31,
        n_created=1,
    )
    index_vector(stack.es, space_id=space, node_key=key, vector=vector)
    archive_node(
        stack.client, stack.cell_session, stack.es, gname=space, space_id=space, node_key=key
    )

    # 推 n 越过归档视界（s=0.31 → n_star=n+1 → n+41 归档，M7 同型用例）
    for _ in range(42):
        ex_ingest.append_experience(
            stack.ex_session,
            stack.redis,
            space_id=space,
            content="filler",
            agent_actor_id="m13-actor",
            account_id="m13-account",
            tau_ms=TAU,
        )

    target = f"{space}_rebuilt"
    plan = rebuild.rebuild_space(
        stack.client,
        stack.cell_session,
        stack.ex_session,
        space_id=space,
        target_gname=target,
        s_resolver=lambda e: 0.31 if e.event_id == e1_id else 1.0,
        ff_config=ff.FFConfig(n_neglect=10_000),
        es=stack.es,
    )

    assert [k for k, _ in plan.archives] == [key]
    assert plan.archives[0][1]["v"] == pytest.approx(vector)
    archived = {row["node_key"]: row for row in list_archived(stack.cell_session, target)}
    assert archived[key]["snapshot"]["v"] == pytest.approx(vector)
