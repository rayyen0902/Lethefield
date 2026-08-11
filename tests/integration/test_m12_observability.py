"""M12 可观测性埋点（开发期最小集）集成测试（真实全栈 + es-ops + Prometheus/Grafana）。

覆盖 M12 验收：
1. 四类指标/schema 全部实现且可在 Prometheus/Grafana 查询（shipper→es-ops 可查、
   /metrics 暴露口、exporter 聚合序列、Prometheus /api/v1/query、Grafana datasource）。
2. 标签纪律由 libs/metrics 代码层强制（space_id/node_key 注册即抛错，单测覆盖）。
3. 聚合走旁路：exporter 数据源 = es-ops 日志流 + 系统表 + 映射表（断言序列存在即证明
   通路；静态边界由 ops/metrics_exporter 模块职责与代码审查保证）。
4. 埋点与训练管线物理分开：日志管线（logschema es_sink）与训练 feed
   （training_feed）无共用埋点函数；③ 收口链路（es-ops → recall_filter → topic → worker）
   端到端验证。
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
import requests
import uvicorn

os.environ["LETHEFIELD_JWT_SECRET"] = "m5-integration-secret"  # 与 m5 同值：进程级 env 互覆防坑
TEST_SECRET = "m5-integration-secret"

from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin  # noqa: E402
from lethefield_api.ex_ingest import ensure_ex_keyspace  # noqa: E402
from lethefield_api.http_app import create_app  # noqa: E402
from lethefield_api.service import ApiContext  # noqa: E402
from lethefield_clients import (  # noqa: E402
    CONTROL_NAMESPACE,
    FEEDS_NAMESPACE,
    TRAINING_TENANT,
    AuthRegistryStore,
    AuthScope,
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceMapping,
    Tier,
    WatermarkState,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    make_feed_publisher,
    pulsar_client,
    redis_client,
    space_ref_of,
)
from lethefield_logschema import LogEvent  # noqa: E402
from lethefield_logschema import configure as logschema_configure  # noqa: E402
from lethefield_logschema import emit as logschema_emit  # noqa: E402
from lethefield_metrics import start_metrics_server  # noqa: E402
from lethefield_rms.schema import ensure_graph_schema  # noqa: E402
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector  # noqa: E402
from lethefield_rms.writer import create_event_node  # noqa: E402
from lethefield_scheduler import pulsar_admin  # noqa: E402
from lethefield_scheduler.config import SchedulerConfig  # noqa: E402
from lethefield_training import recall_filter, worker  # noqa: E402
from lethefield_training.config import TrainingConfig  # noqa: E402
from lethefield_training.hot_store import HotSampleStore  # noqa: E402
from lethefield_training.recall_window import RecallWindow  # noqa: E402
from prometheus_client import REGISTRY  # noqa: E402

SPACE = f"m12_{uuid.uuid4().hex[:8]}"
NODE_KEY = f"ev_{uuid.uuid4().hex[:8]}"
OPS_ES_URL = "http://localhost:9201"  # es-ops（M12 日志管线；shipper 同一约定）


def _token(scopes):
    return jwt.encode(
        {
            "account_id": "acct-m12",
            "space_id": [SPACE],
            "agent_actor_id": "ci",
            "scope": list(scopes),
            "exp": int(time.time()) + 600,
        },
        TEST_SECRET,
        algorithm="HS256",
    )


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    wait_for_gremlin()
    logschema_configure(OPS_ES_URL)  # 本进程 LogEvent 全量进 es-ops（异步批量）
    gremlin = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(gremlin, SPACE)  # 顺带向 es-ops 写 graph_open_completed 事件
    cell_cluster = cassandra_cluster()
    cell_session = cell_cluster.connect()
    store = MappingTableControlPlaneStore(cell_session)
    store.ensure_tables()
    try:
        store.get_cell(local_cell().cell_id)
    except KeyError:
        store.register_cell(local_cell())
    store.register_space(
        SpaceMapping(
            space_id=SPACE,
            cell_id=local_cell().cell_id,
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
            tier=Tier.COLD,
        )
    )
    store.update_cell_watermark(local_cell().cell_id, {"keyspaces": 0.01}, WatermarkState.OPEN)
    ex_cluster = ex_cassandra_cluster()
    ex_session = ex_cluster.connect()
    ensure_ex_keyspace(ex_session, SPACE)
    redis = redis_client()
    es_graph = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es_graph, index=VECTORS_INDEX, dims=4)
    # 一个事件节点 + 向量（retrieve 全链路数据源）
    create_event_node(
        gremlin,
        SPACE,
        node_key=NODE_KEY,
        space_id=SPACE,
        content="M12 观测节点",
        tau_ms=1_720_000_000_000,
        ref_ex=f"ex-{uuid.uuid4().hex[:8]}",
        s=0.9,
        n_created=1,
        agent_actor_id="ci",
    )
    index_vector(
        es_graph,
        index=VECTORS_INDEX,
        node_key=NODE_KEY,
        space_id=SPACE,
        vector=[0.1, 0.2, 0.3, 0.4],
        content="M12 观测节点",
    )
    es_graph.indices.refresh(index=VECTORS_INDEX)

    # 训练 feed 通路（③ 收口链路用）
    config = SchedulerConfig()
    for ns, minutes in (
        (CONTROL_NAMESPACE, config.training_control_retention_minutes),
        (FEEDS_NAMESPACE, config.training_feeds_retention_minutes),
    ):
        pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, ns)
        pulsar_admin.set_retention(
            config.pulsar_admin_url, TRAINING_TENANT, ns, minutes=minutes, size_mb=-1
        )
    pulsar = pulsar_client()

    ctx = ApiContext(
        gremlin=gremlin,
        es=es_graph,
        ex_session=ex_session,
        redis=redis,
        meta_appender=lambda **kw: None,
        mapping_cache=MappingCache(store),
    )
    app = create_app(ctx)
    yield SimpleNamespace(
        gremlin=gremlin,
        store=store,
        cell_session=cell_session,
        ex_session=ex_session,
        redis=redis,
        es_graph=es_graph,
        es_ops=es_client(OPS_ES_URL),
        pulsar=pulsar,
        publish=make_feed_publisher(pulsar),
        registry=AuthRegistryStore(),
        hot_root=tmp_path_factory.mktemp("m12_hot"),
        app=app,
    )
    pulsar.close()
    cell_cluster.shutdown()
    ex_cluster.shutdown()


@contextlib.contextmanager
def _serve(app):
    """在空闲端口起真实 uvicorn（daemon 线程），产出同步 httpx.Client（m5 同款模式）。"""
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


def _wait_es_ops(es_ops, *, event_type: str, marker: str | None = None, timeout_s: float = 20.0):
    """等事件在 es-ops 可查（异步批量 + refresh 延迟），返回命中文档列表。"""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        es_ops.indices.refresh(index="lethefield-logs-*")
        filters = [{"term": {"event_type": event_type}}]
        if marker:
            filters.append({"term": {"space_id": marker}})
        resp = es_ops.search(
            index="lethefield-logs-*", body={"query": {"bool": {"filter": filters}}}
        )
        hits = resp["hits"]["hits"]
        if hits:
            return [h["_source"] for h in hits]
        time.sleep(0.5)
    raise TimeoutError(f"事件未在 es-ops 出现：{event_type} {marker}")


# ---------------------------------------------------------------- 1. 日志管线 shipper


def test_shipper_event_searchable_in_es_ops(stack):
    marker = uuid.uuid4().hex
    logschema_emit(
        LogEvent(
            service="m12-itest",
            event_type="shipper_probe",
            space_id=SPACE,
            payload={"marker": marker},
        ),
        sync=True,
    )
    docs = _wait_es_ops(stack.es_ops, event_type="shipper_probe")
    assert any(d["payload"].get("marker") == marker for d in docs)


# ---------------------------------------------------------------- 2. API /metrics + 请求路径指标


def test_api_metrics_endpoint(stack):
    headers = {"Authorization": f"Bearer {_token(('record', 'retrieve', 'reinforce'))}"}
    with _serve(stack.app) as http:
        resp = http.post(
            "/memory/record",
            json={"space_id": SPACE, "content": "M12 record 探针"},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        resp = http.post(
            "/memory/retrieve",
            json={"space_id": SPACE, "query_text": "M12 观测节点"},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        resp = http.post(
            "/memory/reinforce", json={"space_id": SPACE, "node_key": NODE_KEY}, headers=headers
        )
        assert resp.status_code == 200, resp.text

        metrics = http.get("/metrics")  # 无 Bearer 也可读（运维通道口径）
    assert metrics.status_code == 200
    body = metrics.text
    for name in (
        "lethefield_record_confirm_duration_seconds",
        "lethefield_ex_write_duration_seconds",
        "lethefield_retrieve_stage_duration_seconds",
        "lethefield_ff_theta_filter_ratio",
    ):
        assert name in body, f"{name} 未在 /metrics 暴露"
    assert 'stage="knn"' in body

    # 明细事件进 es-ops：召回（带 event_id/stage_ms）+ reinforce（touched 侧）
    recalls = _wait_es_ops(stack.es_ops, event_type="retrieve_recall_detail", marker=SPACE)
    detail = next(d for d in recalls if d["space_id"] == SPACE)
    assert detail["payload"]["event_id"]
    assert set(detail["payload"]["stage_ms"]) >= {"knn"}
    _wait_es_ops(stack.es_ops, event_type="memory_reinforced", marker=SPACE)


# ---------------------------------------------------------------- 3. exporter 离线聚合


def test_exporter_aggregations(stack):
    from lethefield_decision_log import DecisionLogStore
    from lethefield_metrics_exporter.exporter import ExporterDeps, run_once

    # 留痕线数据源：一条带 Agent 建议的 rejected 决策 + 一条升级事件
    DecisionLogStore().submit(
        title=f"M12 留痕 {uuid.uuid4().hex[:6]}",
        decision="d",
        decided_by="ci",
        agent_suggestion="s",
        outcome="rejected",
        escalation_type="novel_error",
    )
    logschema_emit(
        LogEvent(
            service="lethefield-fs",
            event_type="ff_delta_applied",
            payload={"type": "neglect", "count": 3},
        ),
        sync=True,
    )
    _wait_es_ops(stack.es_ops, event_type="ff_delta_applied")

    deps = ExporterDeps(
        es_ops=stack.es_ops,
        store=stack.store,
        cell_session=stack.cell_session,
        ex_session=stack.ex_session,
        es_graph=stack.es_graph,
    )
    run_once(deps)

    assert (
        REGISTRY.get_sample_value("lethefield_agent_suggestion_total", {"outcome": "rejected"}) >= 1
    )
    assert REGISTRY.get_sample_value("lethefield_escalation_total", {"reason": "novel_error"}) >= 1
    assert REGISTRY.get_sample_value("lethefield_ff_delta_applied_total", {"type": "neglect"}) >= 3
    assert (
        REGISTRY.get_sample_value("lethefield_graph_open_duration_seconds_count", {"type": "cold"})
        >= 1
    )
    # touched rate：本模块 recall + reinforce 同节点命中（其他模块残留召回只拉低不归零）
    rate = REGISTRY.get_sample_value("lethefield_ff_recalled_then_touched_rate")
    assert rate is not None and 0 < rate <= 1
    assert REGISTRY.get_sample_value("lethefield_graph_lru_cache_hit_ratio") is not None
    assert (
        REGISTRY.get_sample_value(
            "lethefield_cell_watermark_ratio",
            {"cell_id": local_cell().cell_id, "dimension": "keyspaces"},
        )
        == 0.01
    )
    storage = REGISTRY.get_sample_value("lethefield_space_storage_bytes", {"tier": "cold"})
    assert storage is not None and storage > 0


# ---------------------------------------------------------------- 4. ③ 收口链路端到端


def test_recall_filter_chain(stack):
    """es-ops 召回明细 → recall_filter（授权闸门）→ 训练 topic → worker 入窗（event_id 去重）。"""
    space_ref = space_ref_of(SPACE)
    stack.registry.grant(space_ref, [AuthScope.CALIBRATION])
    emitted: list[LogEvent] = []
    deps = worker.WorkerDeps(
        store=HotSampleStore(stack.hot_root),
        window=RecallWindow(stack.hot_root / "chain_window.jsonl", w_r3_ms=86_400_000),
        registry=stack.registry,
        emit=emitted.append,
        config=TrainingConfig(),
    )
    runtime = worker.WorkerRuntime(stack.pulsar, deps)
    try:
        runtime.run_once(timeout_ms=500)  # 排水
        forwarded = recall_filter.run_once(
            stack.es_ops,
            registry=stack.registry,
            publish=stack.publish,
            state_path=stack.hot_root / "filter_state.json",
        )
        assert forwarded >= 1  # test_api 的召回明细（SPACE 已授权）
        runtime.run_once(timeout_ms=500)
        assert deps.window.recalled_at(space_ref, NODE_KEY) is not None
        # checkpoint 幂等：再跑一轮无新增转发
        assert (
            recall_filter.run_once(
                stack.es_ops,
                registry=stack.registry,
                publish=stack.publish,
                state_path=stack.hot_root / "filter_state.json",
            )
            == 0
        )
    finally:
        runtime.close()


# ---------------------------------------------------------------- 5. Prometheus / Grafana 可查


def test_prometheus_scrapes_and_grafana_datasource(stack):
    """验收硬指标：序列可在 Prometheus 查询（Grafana datasource 健康）。"""
    start_metrics_server(9104)  # exporter 暴露口（本进程 REGISTRY 已含聚合序列）
    deadline = time.time() + 90  # scrape_interval 15s，留足两个周期余量
    found = False
    while time.time() < deadline:
        try:
            resp = requests.get(
                "http://localhost:9090/api/v1/query",
                params={"query": "lethefield_agent_suggestion_total"},
                timeout=5,
            )
            if resp.json()["data"]["result"]:
                found = True
                break
        except Exception:
            pass
        time.sleep(3)
    assert found, "Prometheus 未 scrape 到 lethefield_ 序列（host.docker.internal:9104）"

    resp = requests.get("http://localhost:3000/api/datasources", auth=("admin", "admin"), timeout=5)
    assert resp.status_code == 200
    assert any(ds["type"] == "prometheus" for ds in resp.json())


# ---------------------------------------------- 6. ex_last_write_age（DMS 顺带发射）


def test_dms_ex_last_write_age_gauge(stack):
    from datetime import UTC, datetime

    from lethefield_clients import touch_last_write
    from lethefield_ingest_dms.__main__ import run_once as dms_run_once
    from lethefield_ingest_dms.config import DmsConfig

    touch_last_write(stack.redis, SPACE, now=datetime.now(UTC))  # 自足：不依赖执行顺序
    dms_run_once(DmsConfig.from_env())
    age_max = REGISTRY.get_sample_value(
        "lethefield_ex_last_write_age_seconds", {"dimension": "max"}
    )
    age_p95 = REGISTRY.get_sample_value(
        "lethefield_ex_last_write_age_seconds", {"dimension": "p95"}
    )
    assert age_max is not None and age_max >= 0
    assert age_p95 is not None and age_p95 <= age_max
