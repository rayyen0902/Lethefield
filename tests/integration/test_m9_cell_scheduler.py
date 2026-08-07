"""M9 Cell 架构 + 租户调度器验收的集成测试（开发文档 §10 五条验收标准）。

真实组件：cassandra-cell（控制面映射表 + 图 keyspace）、cassandra-ex（EX keyspace）、
JanusGraph（图实例驱逐）、ES（rms_vectors 清理）、Pulsar（namespace 生命周期）、Redis。

覆盖：
1. 映射表备份/导出灾难恢复演练（1.0 验收硬指标）；
2. 开通顺序（EX → Pulsar → RMS → 注册）+ 存储失败回滚；
3. 注销严格按"先驱逐计算实例、再删存储"，结束态全链路无残留；
4. 调度器/控制面下线，已有 space 的 record/retrieve/reinforce 仍正常（映射缓存陈旧服务）；
5. 水位状态 open→filling→closed 转换与分配闸门。

space 统一 m9_* 前缀；开通/注销走调度器真实流水线（注销即红线 5 允许的 DROP 路径，
不再需要 make reset 兜底清理）。
"""

import contextlib
import uuid
from types import SimpleNamespace

import pytest
import requests
from cassandra.cluster import NoHostAvailable
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_api import service
from lethefield_api.auth import Claims
from lethefield_api.service import ApiContext
from lethefield_clients import (
    CONTROL_KEYSPACE,
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    Tier,
    WatermarkState,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    export_jsonl,
    gremlin_client,
    keyspace_name,
    local_cell,
    pulsar_client,
    redis_client,
    restore_jsonl,
)
from lethefield_clients.control_plane import CELLS_TABLE, SPACES_TABLE, CellInfo
from lethefield_rms.schema import ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector
from lethefield_rms.writer import create_event_node
from lethefield_scheduler import provision as provision_mod
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import SchedulerConfig
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.provision import ProvisionDeps, ProvisionError, provision_space
from lethefield_scheduler.watermark import NoOpenCellError, refresh_cell, select_cell

TAU = 1_720_000_000_000


def _sid(tag: str) -> str:
    return f"m9_{tag}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="module")
def stack():
    wait_for_gremlin()
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
    # 契约 5 广播通道：训练控制 namespace 就绪 + 真实 Pulsar 客户端（M10 起 destroy 默认走真实广播）
    config = SchedulerConfig()
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE)
    pulsar = pulsar_client()
    yield SimpleNamespace(
        store=store,
        cell_session=cell_session,
        ex_session=ex_cluster.connect(),
        gremlin=gremlin,
        es=es,
        redis=redis_client(),
        config=config,
        pulsar=pulsar,
    )
    store.update_cell_watermark(local_cell().cell_id, {}, WatermarkState.OPEN)  # 水位用例改动复原
    pulsar.close()
    gremlin.close()
    es.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()


def _provision_deps(stack) -> ProvisionDeps:
    return ProvisionDeps(
        store=stack.store,
        gremlin=stack.gremlin,
        ex_session=stack.ex_session,
        cell_session=stack.cell_session,
        config=stack.config,
    )


def _destroy_deps(stack) -> DestroyDeps:
    return DestroyDeps(
        store=stack.store,
        gremlin=stack.gremlin,
        cell_session=stack.cell_session,
        ex_session=stack.ex_session,
        es=stack.es,
        config=stack.config,
        pulsar=stack.pulsar,  # 契约 5 真实广播通道
    )


def _graph_names(stack) -> list[str]:
    return stack.gremlin.submit("ConfiguredGraphFactory.getGraphNames()").all().result()


def _keyspace_exists(session, ks: str) -> bool:
    return (
        session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", (ks,)
        ).one()
        is not None
    )


def _ns_exists(stack, space_id: str) -> bool:
    resp = requests.get(
        f"{stack.config.pulsar_admin_url}/admin/v2/namespaces/"
        f"{stack.config.pulsar_tenant}/{space_id}",
        timeout=10,
    )
    return resp.status_code == 200


# ------------------------------------------- 验收 1：映射表备份/导出灾难恢复演练


def test_backup_restore_drill(stack, tmp_path):
    space_a, space_b = _sid("bka"), _sid("bkb")
    provision_space(_provision_deps(stack), space_a)
    provision_space(_provision_deps(stack), space_b)
    try:
        backup = tmp_path / "mapping.jsonl"
        rows = export_jsonl(stack.store, backup)
        assert rows >= 3  # 至少 1 cell + 2 spaces

        # 灾难：控制面两表全损
        stack.cell_session.execute(f"TRUNCATE {CONTROL_KEYSPACE}.{SPACES_TABLE}")
        stack.cell_session.execute(f"TRUNCATE {CONTROL_KEYSPACE}.{CELLS_TABLE}")
        with pytest.raises(SpaceNotFoundError):
            stack.store.get_space_mapping(space_a)
        assert stack.store.list_spaces() == []

        # 从备份恢复：映射、Cell、sweep 枚举全部复原
        restore_jsonl(stack.store, backup)
        assert stack.store.get_space_mapping(space_a).cell_id == local_cell().cell_id
        assert stack.store.get_cell(local_cell().cell_id).endpoints == local_cell().endpoints
        assert {space_a, space_b} <= set(stack.store.list_spaces())
    finally:
        destroy_space(_destroy_deps(stack), space_a)
        destroy_space(_destroy_deps(stack), space_b)


# ------------------------------------------- 验收 2：开通顺序 + 存储失败回滚


def test_provision_registers_three_way_mapping(stack):
    space_id = _sid("prov")
    try:
        mapping = provision_space(_provision_deps(stack), space_id, tier=Tier.COLD)
        assert mapping.cell_id == local_cell().cell_id
        assert mapping.ex_cluster_id and mapping.pulsar_cluster_id  # 三向映射登记
        assert _keyspace_exists(stack.ex_session, keyspace_name(space_id))  # EX 先行
        assert _ns_exists(stack, space_id)  # Pulsar namespace
        assert space_id in _graph_names(stack)  # RMS 图
        assert _keyspace_exists(stack.cell_session, space_id)  # 图 keyspace
    finally:
        destroy_space(_destroy_deps(stack), space_id)


def test_provision_storage_failure_rolls_back(stack, monkeypatch):
    """RMS 步失败：注册不执行，已完成的 EX/Pulsar 存储被回滚清理。"""
    monkeypatch.setattr(
        provision_mod,
        "ensure_graph_schema",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("模拟 RMS 建图失败")),
    )
    space_id = _sid("rb")
    with pytest.raises(ProvisionError):
        provision_space(_provision_deps(stack), space_id)
    with pytest.raises(SpaceNotFoundError):
        stack.store.get_space_mapping(space_id)  # 注册未执行
    assert not _keyspace_exists(stack.ex_session, keyspace_name(space_id))  # EX 已回滚
    assert not _ns_exists(stack, space_id)  # Pulsar namespace 已回滚
    assert space_id not in _graph_names(stack)


# ------------------------------------------- 验收 3：注销顺序 + 全链路无残留


def test_destroy_evicts_then_drops_no_residue(stack):
    space_id = _sid("dst")
    provision_space(_provision_deps(stack), space_id)
    try:
        # 真实数据足迹：EX 事件 + 图节点 + 向量文档
        ensure_graph_schema(stack.gremlin, space_id)
        create_event_node(
            stack.gremlin,
            space_id,
            node_key="n1",
            space_id=space_id,
            content="m9 destroy node",
            tau_ms=TAU,
            ref_ex="ex-n1",
            s=0.8,
            n_created=1,
        )
        index_vector(stack.es, space_id=space_id, node_key="n1", vector=[1.0, 0.0, 0.0, 0.0])

        broadcast: list[str] = []
        destroy_space(_destroy_deps(stack), space_id, broadcast_destroy=broadcast.append)

        assert broadcast == [space_id]  # 训练管线销毁广播实际调用（注入点验证）
        assert space_id not in _graph_names(stack)  # 计算实例已驱逐（先于 DROP，顺序在单测锁定）
        assert not _keyspace_exists(stack.cell_session, space_id)  # 图 keyspace 已删
        assert not _keyspace_exists(stack.ex_session, keyspace_name(space_id))  # EX 最后删
        assert not _ns_exists(stack, space_id)
        count = stack.es.count(
            index=VECTORS_INDEX, query={"term": {"space_id": space_id}}, routing=space_id
        )
        assert count.body["count"] == 0  # 向量文档清零
        with pytest.raises(SpaceNotFoundError):
            stack.store.get_space_mapping(space_id)  # 映射已清
    finally:
        # 中途失败也要把已开通的 space 收掉（遗留 active 映射会污染后续用例）
        with contextlib.suppress(SpaceNotFoundError):
            destroy_space(_destroy_deps(stack), space_id)


# ------------------------------------------- 验收 4：调度器下线，存量读写不受影响


def test_scheduler_outage_existing_space_read_write(stack):
    space_id = _sid("ha")
    provision_space(_provision_deps(stack), space_id)
    try:
        create_event_node(
            stack.gremlin,
            space_id,
            node_key="n1",
            space_id=space_id,
            content="m9 ha memory",
            tau_ms=TAU,
            ref_ex="ex-n1",
            s=0.9,
            n_created=1,
        )
        index_vector(
            stack.es,
            space_id=space_id,
            node_key="n1",
            vector=[1.0, 0.0, 0.0, 0.0],
            content="m9 ha memory",
        )

        # 控制面独立连接（与图/EX 访问物理隔离）——演练直接把它真实下线
        control_cluster = cassandra_cluster()
        store = MappingTableControlPlaneStore(control_cluster.connect())
        store.ensure_tables()
        # TTL=0：每次解析都先打控制面，故障后只能靠陈旧缓存——真实演练语义
        cache = MappingCache(store, ttl_seconds=0.0)
        ctx = ApiContext(
            gremlin=stack.gremlin,
            es=stack.es,
            ex_session=stack.ex_session,
            redis=stack.redis,
            meta_appender=lambda **kw: None,
            mapping_cache=cache,
        )
        claims = Claims("acct-m9", (space_id,), "claude-code", ("record", "retrieve", "reinforce"))

        service.retrieve(ctx, claims, space_id=space_id, query_text="ha")  # 预热缓存
        control_cluster.shutdown()  # 调度器/控制面整体下线
        with pytest.raises((NoHostAvailable, RuntimeError)):  # 控制面确实不可达（直连证明）
            store.get_space_mapping(space_id)

        # 存量读写全部正常（陈旧映射缓存直连 Cell）
        assert service.record(ctx, claims, space_id=space_id, content="still works")["n"] == 1
        result = service.retrieve(ctx, claims, space_id=space_id, query_text="ha")
        assert any(n["node_key"] == "n1" for n in result["nodes"])
        assert service.reinforce(ctx, claims, space_id=space_id, node_key="n1")["applied"]
        # 缓存未覆盖的 space：无陈旧值可服务，解析失败（不开通、不扩散）
        ghost_claims = Claims("acct-m9", ("ghost_space",), "claude-code", ("retrieve",))
        with pytest.raises((NoHostAvailable, RuntimeError)):
            service.retrieve(ctx, ghost_claims, space_id="ghost_space", query_text="q")
    finally:
        destroy_space(_destroy_deps(stack), space_id)


# ------------------------------------------- 验收 5：水位状态转换与分配闸门


def test_watermark_transitions_gate_allocation(stack):
    cell_id = f"m9cell{uuid.uuid4().hex[:6]}"
    stack.store.register_cell(CellInfo(cell_id=cell_id, endpoints={"cassandra": "x", "es": "y"}))
    try:
        # 模拟负载注入：open → filling → closed 逐级转换并持久化
        cell = refresh_cell(stack.store, cell_id, probe={"keyspaces": 0.5})
        assert cell.watermark_state == WatermarkState.OPEN
        cell = refresh_cell(stack.store, cell_id, probe={"keyspaces": 0.75})
        assert cell.watermark_state == WatermarkState.FILLING
        cell = refresh_cell(stack.store, cell_id, probe={"es_shards": 0.95})
        assert cell.watermark_state == WatermarkState.CLOSED
        assert stack.store.get_cell(cell_id).capacity == {"es_shards": 0.95}  # 持久化

        # filling/closed 后不再向该 Cell 分配（open 的 cell-local 仍可选）
        assert select_cell(stack.store).cell_id == local_cell().cell_id

        # 全部 Cell 非 open：开通拒绝且零副作用
        stack.store.update_cell_watermark(
            local_cell().cell_id, {"keyspaces": 0.8}, WatermarkState.FILLING
        )
        with pytest.raises(NoOpenCellError):
            provision_space(_provision_deps(stack), _sid("gate"))
    finally:
        stack.store.update_cell_watermark(local_cell().cell_id, {}, WatermarkState.OPEN)
