"""M10 跨 Cell 迁移演练（本地档，开发文档 §11 验收第 6 条）。

**按需触发**：依赖 compose `cell2` profile（cassandra-cell-2:9142 / es-graph-2:9300 /
janusgraph-2:8183）。栈不在线时本模块整体 skip 并打印起栈提示——不阻塞常规 CI
（定案：本地档演练不占用常驻内存，profile 默认不进 CI）。

起栈：docker compose --profile cell2 up -d && bash scripts/wait_for_stack.sh

演练内容（全步骤真实执行）：provision（落 cell-local）→ 造样本数据（经验事件 +
纠错 + reinforce + 向量，源图由 M7 重放重建——生产 M15 写入链前的等价物）→
迁移（标 migrating → RMS 目标 Cell 重建 → ES 向量复制 → EX 同集群 scratch 过渡
复制 → 三向校验 → 切映射 → 源侧清理）→ 断言窗口内 record 429 / retrieve 可读 →
目标侧等价校验 → 实测只读窗口 <60s → 演练记录 JSONL 落盘 deploy/baselines/。

已登记缺口（准出档，本演练不覆盖）：EX 跨集群 sstableloader 流式传输（目标端
连接/网络/认证）；API 层多 Cell 连接路由（M9 遗留——演练经 cell2 直连客户端校验
RMS 等价，不经 API 读 RMS）。
"""

import contextlib
import json
import socket
import threading
import time
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from conftest import ES_GRAPH_URL, GREMLIN_ALIAS, GREMLIN_URL, wait_for_gremlin
from lethefield_api import service
from lethefield_api.auth import Claims
from lethefield_api.errors import ApiError, ErrorCode
from lethefield_api.ex_ingest import append_experience, append_meta
from lethefield_clients import (
    CONTROL_NAMESPACE,
    TRAINING_TENANT,
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    SpaceStatus,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    local_cell,
    pulsar_client,
    redis_client,
)
from lethefield_clients.control_plane import CellInfo
from lethefield_clients.ex_n import list_experience_events
from lethefield_rms import rebuild
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index, index_vector, knn_search
from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import SchedulerConfig
from lethefield_scheduler.destroy import DestroyDeps, destroy_space
from lethefield_scheduler.migrate import MigrateDeps, migrate_space
from lethefield_scheduler.provision import ProvisionDeps, provision_space

CELL2_ID = "cell-local-2"
CELL2_GREMLIN_URL = "ws://localhost:8183/gremlin"
CELL2_CASSANDRA_PORT = 9142
CELL2_ES_URL = "http://localhost:9300"
DRILL_RECORD = "deploy/baselines/m10_migration_drill.jsonl"
READ_ONLY_WINDOW_BUDGET_SECONDS = 60.0  # M10 验收：只读窗口目标 <1 分钟


def _cell2_online() -> bool:
    for port in (8183, 9142, 9300):
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            return False
    return True


pytestmark = pytest.mark.skipif(
    not _cell2_online(),
    reason="cell2 profile 未起（按需演练）：docker compose --profile cell2 up -d",
)


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
    config = SchedulerConfig()
    pulsar_admin.ensure_namespace(config.pulsar_admin_url, TRAINING_TENANT, CONTROL_NAMESPACE)
    es = es_client(ES_GRAPH_URL)
    ensure_vectors_index(es, index=VECTORS_INDEX, dims=4)

    # cell2 端点（host 侧连接；JG 容器视角 endpoints 用于建图 backend props）
    cell2_cluster = cassandra_cluster(port=CELL2_CASSANDRA_PORT)
    cell2_gremlin = gremlin_client(CELL2_GREMLIN_URL, GREMLIN_ALIAS)
    cell2_es = es_client(CELL2_ES_URL)
    # cell2 profile 不在 wait_for_stack.sh 覆盖范围——本 fixture 自等 JG-2 就绪
    deadline = time.time() + 180
    while True:
        try:
            cell2_gremlin.submit("'ok'").all().result()
            break
        except Exception:  # noqa: BLE001 — 就绪前连接拒绝属预期，重试到超时
            if time.time() > deadline:
                raise
            time.sleep(2)
    yield SimpleNamespace(
        store=store,
        cell_session=cell_session,
        ex_session=ex_cluster.connect(),
        gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
        es=es,
        redis=redis_client(),
        config=config,
        pulsar=pulsar_client(),
        cell2_session=cell2_cluster.connect(),
        cell2_gremlin=cell2_gremlin,
        cell2_es=cell2_es,
    )
    cell2_gremlin.close()
    cell2_es.close()
    # cell2 注册不残留：后续模块（m9 等）的 select_cell 不应看到一个可能已下线的 Cell
    cell_session.execute("DELETE FROM lethefield_control.cells WHERE cell_id = %s", (CELL2_ID,))
    cell2_cluster.shutdown()
    cell_cluster.shutdown()
    ex_cluster.shutdown()


def _register_cell2(stack) -> None:
    try:
        stack.store.get_cell(CELL2_ID)
    except KeyError:
        stack.store.register_cell(
            CellInfo(
                cell_id=CELL2_ID,
                endpoints={"cassandra": "cassandra-cell-2", "es": "es-graph-2"},
            )
        )


def test_cross_cell_migration_drill(stack):
    space_id = f"m10_drill_{uuid.uuid4().hex[:6]}"
    # 开通（此刻只注册 cell-local，必然落在源 Cell）
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
    assert stack.store.get_space_mapping(space_id).cell_id == local_cell().cell_id
    _register_cell2(stack)

    try:
        # ---- 样本数据：5 经验事件 + 1 纠错（supersedes）+ reinforce 元事件 + 3 向量
        written = [
            append_experience(
                stack.ex_session,
                stack.redis,
                space_id=space_id,
                content=f"演练事件 {i}",
                agent_actor_id="drill",
                account_id="acct",
            )
            for i in range(5)
        ]
        append_experience(  # 纠错事件（ref_conflict → 重建出 supersedes 边）
            stack.ex_session,
            stack.redis,
            space_id=space_id,
            content="纠错：演练事件 0 有误",
            agent_actor_id="drill",
            account_id="acct",
            ref_conflict=rebuild.node_key_of(written[0][0]),
        )
        append_meta(
            stack.ex_session,
            space_id=space_id,
            node_key=rebuild.node_key_of(written[1][0]),
            meta_type="reinforce",
            n_at_event=6,
            agent_actor_id="drill",
            account_id="acct",
        )
        # 源图：M7 EX 重放重建（生产 M15 写入链落地前的图构建等价物）
        rebuild.rebuild_space(
            stack.gremlin,
            stack.cell_session,
            stack.ex_session,
            space_id=space_id,
            target_gname=space_id,
        )
        for event_id, _n in written[:3]:
            index_vector(
                stack.es,
                space_id=space_id,
                node_key=rebuild.node_key_of(event_id),
                vector=[0.1, 0.2, 0.3, 0.4],
                content="演练向量内容",
            )
        source_v, source_e = _graph_counts(stack.gremlin, space_id)
        assert source_v > 0 and source_e > 0

        # ---- API 视角（缓存 TTL=0：每次直达控制面，模拟多进程下的新鲜读）
        # 独立 gremlin 客户端：gremlin_python 基于 tornado 单连接，跨线程共享会死锁
        # （迁移线程占用 stack.gremlin，本线程的 retrieve 必须走自己的连接——M10 实测）
        ctx = service.ApiContext(
            gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
            es=stack.es,
            ex_session=stack.ex_session,
            redis=stack.redis,
            meta_appender=lambda **kw: None,
            mapping_cache=MappingCache(stack.store, ttl_seconds=0.0),
        )
        claims = Claims("acct", (space_id,), "drill", ("record", "retrieve"))

        # ---- 迁移（后台线程），主线程探测窗口语义
        deps = MigrateDeps(
            store=stack.store,
            source_gremlin=stack.gremlin,
            target_gremlin=stack.cell2_gremlin,
            source_cell_session=stack.cell_session,
            target_cell_session=stack.cell2_session,
            source_es=stack.es,
            target_es=stack.cell2_es,
            ex_session=stack.ex_session,
            config=stack.config,
        )
        # ---- 窗口语义探测（确定性）：迁移前写读正常 → 窗口内 record 429 / retrieve 可读
        # → 窗口后写入恢复。不向窗口内注入并发成功写——状态翻转与 EX 重放的先后由
        # 实现保证（翻转在建图/重放之前），并发写只会引入不可控竞态（M10 实测教训）。
        service.record(ctx, claims, space_id=space_id, content="迁移前写入")
        pre_migration_rows = len(list_experience_events(stack.ex_session, space_id=space_id))

        outcome: dict = {}

        def run_migration() -> None:
            try:
                outcome["report"] = migrate_space(deps, space_id, to_cell_id=CELL2_ID)
            except Exception as exc:  # noqa: BLE001 — 汇聚到主线程断言
                outcome["error"] = exc

        thread = threading.Thread(target=run_migration)
        thread.start()
        # 等迁移真正进入窗口（状态翻转对控制面读者可见）
        deadline = time.time() + 30
        while stack.store.get_space_mapping(space_id).status != SpaceStatus.MIGRATING:
            if time.time() > deadline:
                thread.join()
                pytest.fail("迁移未在 30s 内进入 migrating 状态")
            time.sleep(0.05)
        # 窗口内：写入明确 429（生产者重试缓冲语义），只读不受影响
        with pytest.raises(ApiError) as exc_info:
            service.record(ctx, claims, space_id=space_id, content="窗口内写入（应被拒）")
        assert exc_info.value.code == ErrorCode.RATE_LIMITED
        service.retrieve(ctx, claims, space_id=space_id, query_vector=[0.1, 0.2, 0.3, 0.4])
        thread.join()
        if "error" in outcome:
            raise outcome["error"]
        report = outcome["report"]

        # ---- 窗口结束：写入恢复
        service.record(ctx, claims, space_id=space_id, content="迁移后写入")

        # ---- 目标侧等价校验（直连 cell2 客户端；API 多 Cell 路由是已登记缺口）。
        # 目标 = 源 + "迁移前写入"那笔（翻转前完成，被迁移的 EX 重放纳入——EX 是唯一
        # source of truth；迁移后恢复的那笔只进 EX，RMS 异步入链 M14/M15）
        target_v, target_e = _graph_counts(stack.cell2_gremlin, space_id)
        assert (target_v, target_e) == (source_v + 1, source_e + 1)
        target_docs = stack.cell2_es.count(
            index=VECTORS_INDEX, query={"term": {"space_id": space_id}}, routing=space_id
        ).body["count"]
        assert target_docs == 3
        assert (
            len(
                knn_search(
                    stack.cell2_es, space_id=space_id, query_vector=[0.1, 0.2, 0.3, 0.4], k=3
                )
            )
            == 3
        )
        ex_rows_after = len(list_experience_events(stack.ex_session, space_id=space_id))
        # 期望 = 迁移前（含"迁移前写入"）+ 迁移后恢复写入的那一笔；窗口内 429 零写入
        assert ex_rows_after == pre_migration_rows + 1

        # ---- 源侧无残留
        assert (
            space_id
            not in stack.gremlin.submit("ConfiguredGraphFactory.getGraphNames()").all().result()
        )
        assert (
            stack.es.options(ignore_status=(404,))
            .count(index=VECTORS_INDEX, query={"term": {"space_id": space_id}}, routing=space_id)
            .body["count"]
            == 0
        )

        # ---- 映射与窗口实测
        mapping = stack.store.get_space_mapping(space_id)
        assert mapping.cell_id == CELL2_ID
        assert mapping.status == SpaceStatus.ACTIVE
        assert report.read_only_window_seconds < READ_ONLY_WINDOW_BUDGET_SECONDS

        # ---- 演练记录落盘（M10 验收：实测只读窗口时长并记录）
        record = {
            "ts": datetime.now(UTC).isoformat(),
            "drill": "m10_cross_cell_migration_local",
            "space_id": space_id,
            "source_cell": report.source_cell_id,
            "target_cell": report.target_cell_id,
            "read_only_window_seconds": report.read_only_window_seconds,
            "budget_seconds": READ_ONLY_WINDOW_BUDGET_SECONDS,
            "step_seconds": report.step_seconds,
            "rms_vertices": report.rms_vertices,
            "rms_edges": report.rms_edges,
            "vector_docs": report.vector_docs,
            "ex_experience_rows": report.ex_experience_rows,
            "ex_meta_rows": report.ex_meta_rows,
            "known_gaps": [
                "EX 跨集群 sstableloader 流式传输（准出档）",
                "API 层多 Cell 连接路由（M9 遗留，演练经 cell2 直连校验）",
            ],
        }
        with open(DRILL_RECORD, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        # 清理：迁移后图在 cell2——destroy 依赖指向 cell2 客户端（顺带演练非默认 Cell 注销）
        # SpaceNotFoundError = 开通本身失败时已无映射，无需清理
        with contextlib.suppress(SpaceNotFoundError):
            destroy_space(
                DestroyDeps(
                    store=stack.store,
                    gremlin=stack.cell2_gremlin,
                    cell_session=stack.cell2_session,
                    ex_session=stack.ex_session,
                    es=stack.cell2_es,
                    config=stack.config,
                    pulsar=stack.pulsar,
                ),
                space_id,
            )


def _graph_counts(gremlin, gname: str) -> tuple[int, int]:
    vertices = (
        gremlin.submit(
            "def g = ConfiguredGraphFactory.open(gname); g.traversal().V().count().next()",
            {"gname": gname},
        )
        .all()
        .result()[0]
    )
    edges = (
        gremlin.submit(
            "def g = ConfiguredGraphFactory.open(gname); g.traversal().E().count().next()",
            {"gname": gname},
        )
        .all()
        .result()[0]
    )
    return int(vertices), int(edges)
