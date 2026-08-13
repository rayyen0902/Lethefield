"""M17 运维操作面验收的集成测试（开发文档 §18 四条验收标准）。

真实组件：全栈（cassandra-cell/ex、JanusGraph、ES、Pulsar、Postgres）。
CLI 走真实 `lethefield_ops_cli.__main__.main`（内部自建连接，与人工执行同路径）。

验收映射：
1. CLI 覆盖全部人工触发点逐条可执行 —— status/set-tier/auth revoke/cell watermark/
   cell register/destroy 逐条跑通（迁移三类入口的单测覆盖组装逻辑；真实迁移演练归
   M10 drill，不在本套件重复起 cell2）。
2. 静态检查无全局命令 —— ops/ops_cli/tests/test_no_global_commands.py（随 make test）。
3. 处置类命令执行后留痕可查（操作人/参数/结果齐全）——各用例断言 decision_log。
4. 销毁处置端到端演练 —— CLI 触发 → M9/M10 真实流水线（含契约 5 真实广播）→
   无残留 → 留痕可查。
"""

import json
import os
import uuid
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_clients import (
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    AuthRegistryStore,
    AuthScope,
    AuthStatus,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    Tier,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pg_connection,
    space_ref_of,
)
from lethefield_decision_log import DecisionLogStore
from lethefield_ops_cli.__main__ import main as ops_cli
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector
from lethefield_rms.writer import create_edge, create_event_node
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import DEFAULT_CONFIG
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.provision import ProvisionDeps, provision_space
from lethefield_training.hot_store import HotSampleStore
from lethefield_training.sample import TrainingSample

OP = f"m17_itest_{uuid.uuid4().hex[:6]}"
SPACE_A = f"m17a_{uuid.uuid4().hex[:8]}"
SPACE_B = f"m17b_{uuid.uuid4().hex[:8]}"
TMP_CELL = f"cell_m17_{uuid.uuid4().hex[:6]}"
N_BASE = 100
TAU = 1_720_000_000_000


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    wait_for_gremlin()
    cell_cluster = cassandra_cluster()
    ex_cluster = ex_cassandra_cluster()
    store = MappingTableControlPlaneStore(cell_cluster.connect())
    store.ensure_tables()
    try:
        store.get_cell(local_cell().cell_id)
    except KeyError:
        store.register_cell(local_cell())
    # 契约 5 控制面（销毁真实广播通道，同 scheduler bootstrap）
    pulsar_admin.ensure_namespace(
        DEFAULT_CONFIG.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE
    )
    gremlin = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)

    deps = ProvisionDeps(
        store=store,
        gremlin=gremlin,
        ex_session=ex_cluster.connect(),
        cell_session=cell_cluster.connect(),
    )
    provision_space(deps, SPACE_A)
    provision_space(deps, SPACE_B)

    # SPACE_A 数据面：2 顶点 + 1 边 + 2 向量（status 配额用量非零）
    create_event_node(
        gremlin,
        SPACE_A,
        node_key="n1",
        space_id=SPACE_A,
        content="m17 a1",
        tau_ms=TAU,
        ref_ex="ex-1",
        s=0.5,
        n_created=N_BASE,
    )
    create_event_node(
        gremlin,
        SPACE_A,
        node_key="n2",
        space_id=SPACE_A,
        content="m17 a2",
        tau_ms=TAU,
        ref_ex="ex-2",
        s=0.6,
        n_created=N_BASE + 1,
    )
    create_edge(gremlin, SPACE_A, space_id=SPACE_A, from_key="n1", to_key="n2", label="temporal")
    index_vector(es, space_id=SPACE_A, node_key="n1", vector=[1.0, 0.0, 0.0, 0.0], content="m17 a1")
    index_vector(
        es, space_id=SPACE_A, node_key="n2", vector=[0.9, 0.44, 0.0, 0.0], content="m17 a2"
    )

    # 授权 + 热层存量样本（撤回处置的真实存量）
    hot_root = tmp_path_factory.mktemp("m17_hot")
    old_hot = os.environ.get("LETHEFIELD_TRAINING_HOT_ROOT")
    os.environ["LETHEFIELD_TRAINING_HOT_ROOT"] = str(hot_root)
    space_ref = space_ref_of(SPACE_A)
    AuthRegistryStore().grant(space_ref, [AuthScope.CALIBRATION])
    HotSampleStore(hot_root).append(
        [
            TrainingSample.new(
                source="ex_derived",
                rule="R3",
                space_ref=space_ref,
                problem={"c": "m17 存量"},
                diagnosis={},
                decision={},
                outcome={},
                auth_scope="granted",
            )
        ]
    )

    yield SimpleNamespace(store=store, gremlin=gremlin, es=es, hot_root=hot_root)

    # 清理：SPACE_A 销毁（广播空操作，契约 5 真实路径由 SPACE_B 用例覆盖）+ PG 痕迹
    destroy_space(
        DestroyDeps(
            store=store,
            gremlin=gremlin,
            cell_session=cell_cluster.connect(),
            ex_session=ex_cluster.connect(),
            es=es,
        ),
        SPACE_A,
        broadcast_destroy=lambda space_id: None,
    )
    with pg_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM auth_registry WHERE space_ref = %s", (space_ref,))
        cur.execute("DELETE FROM decision_log WHERE decided_by = %s", (OP,))
    # cells 表在 Cassandra 控制面 keyspace（不在 PG——PG DELETE 会 teardown 报错残留路由）
    cell_cluster.connect().execute(
        "DELETE FROM lethefield_control.cells WHERE cell_id = %s", (TMP_CELL,)
    )
    gremlin.close()
    es.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()
    if old_hot is None:
        del os.environ["LETHEFIELD_TRAINING_HOT_ROOT"]
    else:
        os.environ["LETHEFIELD_TRAINING_HOT_ROOT"] = old_hot


def _audit_records() -> list:
    return [r for r in DecisionLogStore().list(limit=200) if r.decided_by == OP]


def test_status_and_set_tier(stack, capsys):
    """验收 1：space 状态查询 + tier 升降逐条可执行；验收 3：留痕可查。"""
    assert ops_cli(["--operator", OP, "space", "status", "--space", SPACE_A]) == 0
    out = capsys.readouterr().out
    assert f"space {SPACE_A} cell=" in out and "status=active tier=cold" in out
    # 图计数是进程级共享 TTL 缓存的近似值（红线 2 定案语义）：fixture 内 writer 配额
    # 预检刚把空图计数写进缓存，TTL 窗口内 status 读到的是缓存旧值——不断言精确数，
    # 只断言三路计数输出与近似语义标注齐全
    assert "vertices=" in out and "edges=" in out and "vectors=2" in out
    assert "近似" in out

    assert (
        ops_cli(["--operator", OP, "space", "set-tier", "--space", SPACE_A, "--tier", "hot"]) == 0
    )
    assert stack.store.get_space_mapping(SPACE_A).tier == Tier.HOT

    records = _audit_records()
    status_rec = next(r for r in records if "space status" in r.title)
    assert SPACE_A in status_rec.title
    assert json.loads(status_rec.context)["result"] == "ok"
    tier_rec = next(r for r in records if "set-tier" in r.title)
    assert tier_rec.decided_by == OP
    assert "hot" in tier_rec.decision


def test_auth_revoke(stack, capsys):
    """验收 1/3：授权撤回处置 = 注册表撤回 + 热层存量 scrub，留痕可查。"""
    assert ops_cli(["--operator", OP, "auth", "revoke", "--space", SPACE_A]) == 0
    entry = AuthRegistryStore().get(space_ref_of(SPACE_A))
    assert entry.status == AuthStatus.REVOKED
    manifest = HotSampleStore(stack.hot_root).manifest(space_ref_of(SPACE_A))
    assert manifest and all(e.scrubbed for e in manifest)
    out = capsys.readouterr().out
    assert "存量处置 1 条" in out
    rec = next(r for r in _audit_records() if "auth revoke" in r.title)
    assert json.loads(rec.context)["result"] == "ok"


def test_cell_watermark_and_register(stack, capsys):
    """验收 1：Cell 水位查看（含 --refresh 探测）与新 Cell 筹备触发。"""
    assert ops_cli(["--operator", OP, "cell", "watermark", "--cell", "cell-local"]) == 0
    assert "state=" in capsys.readouterr().out

    assert (
        ops_cli(["--operator", OP, "cell", "watermark", "--cell", "cell-local", "--refresh"]) == 0
    )
    assert stack.store.get_cell("cell-local").capacity  # 探测后 capacity 非空

    assert (
        ops_cli(
            [
                "--operator",
                OP,
                "cell",
                "register",
                "--cell-id",
                TMP_CELL,
                "--endpoint",
                "cassandra=cassandra-cell",
                "--endpoint",
                "es=es-graph",
            ]
        )
        == 0
    )
    cell = stack.store.get_cell(TMP_CELL)
    assert cell.endpoints == {"cassandra": "cassandra-cell", "es": "es-graph"}


def test_destroy_end_to_end(stack):
    """验收 4：销毁处置端到端演练——CLI 触发 → 真实流水线（契约 5 真实广播）→
    无残留 → 留痕可查（操作人/参数/结果齐全）。"""
    assert (
        ops_cli(
            [
                "--operator",
                OP,
                "space",
                "destroy",
                "--space",
                SPACE_B,
                "--reason",
                "用户销毁请求演练",
            ]
        )
        == 0
    )
    with pytest.raises(SpaceNotFoundError):
        stack.store.get_space_mapping(SPACE_B)  # 无残留（destroy 内部已做残留校验）

    rec = next(r for r in _audit_records() if "space destroy" in r.title)
    assert rec.decided_by == OP
    assert SPACE_B in rec.decision
    assert rec.rationale == "用户销毁请求演练"
    context = json.loads(rec.context)
    assert context["result"] == "ok" and "已注销" in context["detail"]


def test_audit_fail_closed(stack, monkeypatch, capsys):
    """硬约束 2：留痕库不可达 → 命令拒绝执行，零业务副作用（tier 不变）。"""
    monkeypatch.setenv(
        "LETHEFIELD_PG_DSN", "host=localhost port=59999 dbname=lethefield connect_timeout=1"
    )
    assert (
        ops_cli(["--operator", OP, "space", "set-tier", "--space", SPACE_A, "--tier", "premium"])
        == 1
    )
    assert "拒绝执行" in capsys.readouterr().err
    # 被拒的 premium 调整未生效（不断言具体 tier 值——本用例可独立于 set-tier 用例运行）
    assert stack.store.get_space_mapping(SPACE_A).tier != Tier.PREMIUM
