"""M10 契约 5 销毁广播集成测试（真实 Pulsar，默认栈）。

覆盖 M10 验收第 4 条：注销流程第 4 步**实际调用**训练管线销毁指令接口——
生产者等 broker ack（真实 broker），最小接收 sink 实际收到指令并落接收记录
（决策留痕可查）；训练 tenant/namespace 与业务流隔离、retention 独立配置。
"""

import uuid
from types import SimpleNamespace

import pytest
import requests
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_clients import (
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    SpaceStatus,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pulsar_client,
    space_ref_of,
)
from lethefield_clients.training_control import control_topic
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import SchedulerConfig
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.destroy_broadcast import BroadcastError
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_scheduler.training_control_sink import run_once


def _sid(tag: str) -> str:
    return f"m10_{tag}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="module")
def stack():
    wait_for_gremlin()
    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    store = MappingTableControlPlaneStore(cell_cluster.connect())
    store.ensure_tables()
    try:
        store.get_cell(local_cell().cell_id)
    except KeyError:
        store.register_cell(local_cell())
    config = SchedulerConfig()
    # 契约 5 通道：训练控制 namespace（独立 tenant + 审计级 retention）
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE)
    pulsar_admin.set_retention(
        config.pulsar_admin_url,
        TRAINING_TENANT,
        CONTROL_NAMESPACE,
        minutes=config.training_control_retention_minutes,
        size_mb=-1,
    )
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)
    pulsar = pulsar_client()
    yield SimpleNamespace(
        store=store,
        cell_session=cell_cluster.connect(),
        ex_session=ex_cluster.connect(),
        gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
        es=es,
        config=config,
        pulsar=pulsar,
    )
    pulsar.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()


def _provision(stack, space_id: str) -> None:
    provision_space(
        ProvisionDeps(
            store=stack.store,
            gremlin=stack.gremlin,
            ex_session=stack.ex_session,
            cell_session=stack.cell_session,
            config=stack.config,
        ),
        space_id,
    )


def _destroy_deps(stack, *, pulsar=None) -> DestroyDeps:
    return DestroyDeps(
        store=stack.store,
        gremlin=stack.gremlin,
        cell_session=stack.cell_session,
        ex_session=stack.ex_session,
        es=stack.es,
        config=stack.config,
        pulsar=pulsar if pulsar is not None else stack.pulsar,
    )


def test_destroy_broadcast_reaches_training_sink(stack):
    """验收：destroy 第 4 步真实广播（broker ack）→ sink 实际接收 + 接收记录可查。"""
    space_id = _sid("bcast")
    _provision(stack, space_id)
    sink = pulsar_client()
    try:
        run_once(sink, emit=lambda e: None, timeout_ms=500)  # 先建 durable 订阅（离线不丢指令）
        destroy_space(_destroy_deps(stack), space_id)  # 真实广播（无注入）

        received = []
        processed = run_once(sink, emit=received.append, timeout_ms=10_000)
        assert processed == 1
        (event,) = received
        assert event.event_type == "space_destroy_received"
        assert event.payload["space_ref"] == space_ref_of(space_id)
        assert space_id not in event.to_jsonl()  # 不明文暴露 space_id（契约 5 space_ref 哈希）
    finally:
        sink.close()
        # 主路径结束态：space 已注销，映射无残留（destroy 内部无残留校验已过）
        with pytest.raises(SpaceNotFoundError):
            stack.store.get_space_mapping(space_id)


def test_broadcast_failure_aborts_then_retry_completes(stack):
    """契约 5 硬约束 1：广播失败 → 第 5 步不执行（映射留 destroying）；修复后重跑续做。"""
    space_id = _sid("retry")
    _provision(stack, space_id)
    sink = pulsar_client()
    dead = pulsar_client()
    dead.close()  # 已关闭的客户端模拟 broker 不可达（create_producer 立即失败）
    try:
        run_once(sink, emit=lambda e: None, timeout_ms=500)  # durable 订阅先建
        with pytest.raises(BroadcastError):
            destroy_space(_destroy_deps(stack, pulsar=dead), space_id)
        mapping = stack.store.get_space_mapping(space_id)  # 第 5 步未执行：映射仍在
        assert mapping.status == SpaceStatus.DESTROYING

        destroy_space(_destroy_deps(stack), space_id)  # 修复通道后重跑（步骤 1-3 幂等）
        received = []
        assert run_once(sink, emit=received.append, timeout_ms=10_000) == 1
        assert received[0].payload["space_ref"] == space_ref_of(space_id)
    finally:
        sink.close()


def test_training_tenant_isolated_with_independent_retention(stack):
    """训练 tenant 与业务流隔离、retention 独立（契约 5 既定原则落地校验）。"""
    admin = stack.config.pulsar_admin_url
    tenants = requests.get(f"{admin}/admin/v2/tenants", timeout=10).json()
    assert TRAINING_TENANT in tenants
    assert stack.config.pulsar_tenant in tenants
    assert stack.config.pulsar_tenant != TRAINING_TENANT

    retention = requests.get(
        f"{admin}/admin/v2/namespaces/{TRAINING_TENANT}/{CONTROL_NAMESPACE}/retention",
        timeout=10,
    ).json()
    assert retention["retentionTimeInMinutes"] == stack.config.training_control_retention_minutes

    namespaces = requests.get(f"{admin}/admin/v2/namespaces/{TRAINING_TENANT}", timeout=10).json()
    assert namespaces == [f"{TRAINING_TENANT}/{CONTROL_NAMESPACE}"]  # 无业务 namespace 混入
    assert control_topic().startswith(f"persistent://{TRAINING_TENANT}/")
