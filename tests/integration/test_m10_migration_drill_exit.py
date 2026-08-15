"""M10 迁移演练准出档（开发文档 §11 验收第 6 条②）：EX 跨集群 sstableloader 流式传输。

**按需触发**：依赖 compose `cell2` profile（cassandra-cell-2:9142 / es-graph-2:9300 /
janusgraph-2:8183）+ `ex2` profile（cassandra-ex-2:9143，独立第二 EX 集群）。
栈不在线时本模块整体 skip——不阻塞常规 CI（定案：演练 profile 默认不进 CI）。

起栈：docker compose --profile cell2 --profile ex2 up -d && bash scripts/wait_for_stack.sh

与本地档（test_m10_migration_drill.py）的差异只在 EX 侧：
- 本地档：EX 同集群 scratch 过渡复制；
- 准出档（本模块）：EX 跨集群 sstableloader 流式传输——源集群 nodetool snapshot →
  源容器内整理 loader 目录布局 → 目标集群建同名 keyspace schema →
  `sstableloader -d cassandra-ex-2` 逐表流式加载（容器网络直连 storage 端口）→
  目标侧行数校验。传输编排（snapshot/loader 调用）属演练工具链，经
  `migrate_space(ex_transfer=..., to_ex_cluster_id=...)` 注入点进入流水线，
  迁移顺序/校验口径/切映射语义与本地档完全一致。

Pulsar 侧按定案为空操作（全局单池 1.0，namespace 不随 Cell 迁移），记录 notes 字段。
已登记缺口（本演练仍不覆盖）：API 层多 Cell 连接路由（M9 遗留——迁移后校验经
cell2 / EX-2 直连客户端，不经 API 路由）。
"""

import contextlib
import json
import socket
import subprocess
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
from lethefield_clients.ex_n import (
    EXPERIENCE_TABLE,
    META_TABLE,
    ensure_ex_keyspace_named,
    keyspace_name,
    list_experience_events,
    n_key,
)
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
EX2_CLUSTER_ID = "ex-local-2"
EX2_CASSANDRA_PORT = 9143
SNAPSHOT_TAG = "migexit"
DRILL_RECORD = "deploy/baselines/m10_migration_drill_exit.jsonl"
READ_ONLY_WINDOW_BUDGET_SECONDS = 60.0  # M10 验收：只读窗口目标 <1 分钟


def _drill_stack_online() -> bool:
    for port in (8183, 9142, 9300, EX2_CASSANDRA_PORT):
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        except OSError:
            return False
    return True


pytestmark = pytest.mark.skipif(
    not _drill_stack_online(),
    reason="cell2/ex2 profile 未起（按需演练）：docker compose --profile cell2 --profile ex2 up -d",
)


def _compose(*args: str) -> subprocess.CompletedProcess:
    """docker compose 调用（演练工具链；失败即失败，不静默）。"""
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)


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

    cell2_cluster = cassandra_cluster(port=CELL2_CASSANDRA_PORT)
    cell2_gremlin = gremlin_client(CELL2_GREMLIN_URL, GREMLIN_ALIAS)
    cell2_es = es_client(CELL2_ES_URL)
    ex2_cluster = ex_cassandra_cluster(port=EX2_CASSANDRA_PORT)
    # profile 服务不在 wait_for_stack.sh 覆盖范围——本 fixture 自等 JG-2 / EX-2 就绪
    deadline = time.time() + 180
    while True:
        try:
            cell2_gremlin.submit("'ok'").all().result()
            ex2_session = ex2_cluster.connect()
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
        ex2_session=ex2_session,
    )
    cell2_gremlin.close()
    cell2_es.close()
    # cell2 注册不残留：后续模块（m9 等）的 select_cell 不应看到一个可能已下线的 Cell
    cell_session.execute("DELETE FROM lethefield_control.cells WHERE cell_id = %s", (CELL2_ID,))
    cell2_cluster.shutdown()
    cell_cluster.shutdown()
    ex_cluster.shutdown()
    ex2_cluster.shutdown()


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


def _count_ex(session, ks: str) -> tuple[int, int]:
    experience = session.execute(f"SELECT COUNT(*) AS c FROM {ks}.{EXPERIENCE_TABLE}").one().c
    meta = session.execute(f"SELECT COUNT(*) AS c FROM {ks}.{META_TABLE}").one().c
    return int(experience), int(meta)


def _max_n(session, ks: str) -> int:
    row = session.execute(f"SELECT MAX(n) AS m FROM {ks}.{EXPERIENCE_TABLE}").one()
    return int(row.m) if row.m is not None else 0


def _ex_transfer_cross_cluster(stack, space_id: str, metrics: dict):
    """构造准出档 EX 迁移步骤：snapshot → sstableloader 跨集群流式传输 → 行数校验。

    工具链编排（docker exec / nodetool / sstableloader）属演练层；迁移流水线经
    migrate_space 的 ex_transfer 注入点调用本闭包，流水线语义不变。
    """
    ks = keyspace_name(space_id)

    def transfer() -> tuple[int, int]:
        started = time.monotonic()
        # 1. 源集群一致性快照（FLUSH + 快照点，sstableloader 只搬 sstable）
        _compose("exec", "-T", "cassandra-ex", "nodetool", "snapshot", "-t", SNAPSHOT_TAG, ks)
        # 2. 源容器内整理 loader 目录布局 /tmp/migload/<ks>/<table>/（EX 表两表固定，
        #    表目录名带 -<uuid> 后缀，glob 取值；只搬快照点，不碰在线 sstable）
        layout_cmds = [f"rm -rf /tmp/migload && mkdir -p /tmp/migload/{ks}"]
        for table in (EXPERIENCE_TABLE, META_TABLE):
            layout_cmds.append(
                f"mkdir -p /tmp/migload/{ks}/{table} && "
                f"cp /var/lib/cassandra/data/{ks}/{table}-*/snapshots/{SNAPSHOT_TAG}/* "
                f"/tmp/migload/{ks}/{table}/"
            )
        _compose("exec", "-T", "cassandra-ex", "bash", "-c", " && ".join(layout_cmds))
        metrics["ex_transfer_bytes"] = int(
            _compose(
                "exec", "-T", "cassandra-ex", "du", "-sb", f"/tmp/migload/{ks}"
            ).stdout.split()[0]
        )
        # 3. 目标集群建同名 keyspace + 两表 schema（DDL 单点 ensure_ex_keyspace_named）
        ensure_ex_keyspace_named(stack.ex2_session, ks)
        # 4. sstableloader 逐表流式传输（源容器 → cassandra-ex-2 storage 端口）
        for table in (EXPERIENCE_TABLE, META_TABLE):
            _compose(
                "exec",
                "-T",
                "cassandra-ex",
                "sstableloader",
                "-d",
                "cassandra-ex-2",
                f"/tmp/migload/{ks}/{table}",
            )
        metrics["ex_transfer_seconds"] = round(time.monotonic() - started, 3)
        # 5. 目标侧行数校验（EX 完整性口径 = 两表行数与源快照点一致）
        expected = _count_ex(stack.ex_session, ks)
        actual = _count_ex(stack.ex2_session, ks)
        if actual != expected:
            raise AssertionError(f"EX 跨集群传输校验失败：{ks} 目标 {actual} ≠ 源 {expected}")
        # 6. 工具链现场清理（快照点 + loader 目录；失败不影响已完成的传输）
        _compose("exec", "-T", "cassandra-ex", "nodetool", "clearsnapshot", "-t", SNAPSHOT_TAG)
        _compose("exec", "-T", "cassandra-ex", "rm", "-rf", "/tmp/migload")
        return actual

    return transfer


def test_cross_cluster_ex_migration_drill_exit(stack):
    space_id = f"m10_exit_{uuid.uuid4().hex[:6]}"
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
    ks = keyspace_name(space_id)

    try:
        # ---- 样本数据：20 经验事件 + 1 纠错（supersedes）+ reinforce 元事件 + 5 向量
        written = [
            append_experience(
                stack.ex_session,
                stack.redis,
                space_id=space_id,
                content=f"准出档演练事件 {i}",
                agent_actor_id="drill",
                account_id="acct",
            )
            for i in range(20)
        ]
        append_experience(
            stack.ex_session,
            stack.redis,
            space_id=space_id,
            content="纠错：准出档演练事件 0 有误",
            agent_actor_id="drill",
            account_id="acct",
            ref_conflict=rebuild.node_key_of(written[0][0]),
        )
        append_meta(
            stack.ex_session,
            space_id=space_id,
            node_key=rebuild.node_key_of(written[1][0]),
            meta_type="reinforce",
            n_at_event=21,
            agent_actor_id="drill",
            account_id="acct",
        )
        rebuild.rebuild_space(
            stack.gremlin,
            stack.cell_session,
            stack.ex_session,
            space_id=space_id,
            target_gname=space_id,
        )
        for event_id, _n in written[:5]:
            index_vector(
                stack.es,
                space_id=space_id,
                node_key=rebuild.node_key_of(event_id),
                vector=[0.1, 0.2, 0.3, 0.4],
                content="准出档演练向量内容",
            )
        source_v, source_e = _graph_counts(stack.gremlin, space_id)
        assert source_v > 0 and source_e > 0

        # ---- API 视角（缓存 TTL=0；独立 gremlin 客户端防 tornado 单连接跨线程死锁）
        ctx = service.ApiContext(
            gremlin=gremlin_client(GREMLIN_URL, GREMLIN_ALIAS),
            es=stack.es,
            ex_session=stack.ex_session,
            redis=stack.redis,
            meta_appender=lambda **kw: None,
            mapping_cache=MappingCache(stack.store, ttl_seconds=0.0),
        )
        claims = Claims("acct", (space_id,), "drill", ("record", "retrieve"))

        # ---- 迁移（后台线程；EX 跨集群 sstableloader 注入 + EX 归属切换）
        transfer_metrics: dict = {}
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
        service.record(ctx, claims, space_id=space_id, content="迁移前写入")
        pre_migration_rows = len(list_experience_events(stack.ex_session, space_id=space_id))
        source_max_n = _max_n(stack.ex_session, ks)

        outcome: dict = {}

        def run_migration() -> None:
            try:
                outcome["report"] = migrate_space(
                    deps,
                    space_id,
                    to_cell_id=CELL2_ID,
                    to_ex_cluster_id=EX2_CLUSTER_ID,
                    ex_transfer=_ex_transfer_cross_cluster(stack, space_id, transfer_metrics),
                )
            except Exception as exc:  # noqa: BLE001 — 汇聚到主线程断言
                outcome["error"] = exc

        thread = threading.Thread(target=run_migration)
        thread.start()
        deadline = time.time() + 30
        while stack.store.get_space_mapping(space_id).status != SpaceStatus.MIGRATING:
            if time.time() > deadline:
                thread.join()
                pytest.fail("迁移未在 30s 内进入 migrating 状态")
            time.sleep(0.05)
        with pytest.raises(ApiError) as exc_info:
            service.record(ctx, claims, space_id=space_id, content="窗口内写入（应被拒）")
        assert exc_info.value.code == ErrorCode.RATE_LIMITED
        service.retrieve(ctx, claims, space_id=space_id, query_vector=[0.1, 0.2, 0.3, 0.4])
        thread.join()
        if "error" in outcome:
            raise outcome["error"]
        report = outcome["report"]

        # ---- 映射：Cell 与 EX 归属双双切换到目标
        mapping = stack.store.get_space_mapping(space_id)
        assert mapping.cell_id == CELL2_ID
        assert mapping.ex_cluster_id == EX2_CLUSTER_ID
        assert mapping.status == SpaceStatus.ACTIVE
        assert report.read_only_window_seconds < READ_ONLY_WINDOW_BUDGET_SECONDS

        # ---- 目标侧校验（直连 cell2 / EX-2 客户端；API 多 Cell 路由是已登记缺口）
        # RMS：目标图 == 源图 + "迁移前写入"那笔（EX 重放纳入）
        target_v, target_e = _graph_counts(stack.cell2_gremlin, space_id)
        assert (target_v, target_e) == (source_v + 1, source_e + 1)
        # 向量：复制完整且 kNN 可查
        target_docs = stack.cell2_es.count(
            index=VECTORS_INDEX, query={"term": {"space_id": space_id}}, routing=space_id
        ).body["count"]
        assert target_docs == 5
        knn_hits = knn_search(
            stack.cell2_es, space_id=space_id, query_vector=[0.1, 0.2, 0.3, 0.4], k=5
        )
        assert len(knn_hits) == 5
        # EX：目标集群两表行数 == 源快照点（迁移前写入已纳入）
        assert _count_ex(stack.ex2_session, ks) == (pre_migration_rows, 1)
        # n 连续性：Redis 权威计数 == 目标集群 MAX(n)
        redis_n = int(stack.redis.get(n_key(space_id)))
        assert redis_n == _max_n(stack.ex2_session, ks) == source_max_n
        # 迁移后写入恢复（直连目标 EX 集群）：n 接续不缺口不重复
        _event_id, new_n = append_experience(
            stack.ex2_session,
            stack.redis,
            space_id=space_id,
            content="迁移后写入（目标 EX 集群）",
            agent_actor_id="drill",
            account_id="acct",
        )
        assert new_n == source_max_n + 1
        assert _count_ex(stack.ex2_session, ks) == (pre_migration_rows + 1, 1)

        # ---- 源侧宽限期清理（grace=0）：RMS 图/向量由流水线已清，EX 源 keyspace
        # 按设计 §11 迁移第 5 步"源侧宽限期后按注销流程销毁"在本演练即期执行
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
        stack.ex_session.execute(f"DROP KEYSPACE IF EXISTS {ks}", timeout=120)
        assert not any(
            r.keyspace_name == ks
            for r in stack.ex_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        )

        # ---- 演练记录落盘（M10 验收：实测数据入演练记录）
        record = {
            "ts": datetime.now(UTC).isoformat(),
            "drill": "m10_cross_cell_migration_exit",
            "space_id": space_id,
            "source_cell": report.source_cell_id,
            "target_cell": report.target_cell_id,
            "source_ex_cluster": "ex-local",
            "target_ex_cluster": EX2_CLUSTER_ID,
            "read_only_window_seconds": report.read_only_window_seconds,
            "budget_seconds": READ_ONLY_WINDOW_BUDGET_SECONDS,
            "step_seconds": report.step_seconds,
            "rms_vertices": report.rms_vertices,
            "rms_edges": report.rms_edges,
            "vector_docs": report.vector_docs,
            "ex_experience_rows": report.ex_experience_rows,
            "ex_meta_rows": report.ex_meta_rows,
            "ex_transfer": {
                "method": "nodetool snapshot + sstableloader -d cassandra-ex-2（逐表流式）",
                "bytes": transfer_metrics["ex_transfer_bytes"],
                "seconds": transfer_metrics["ex_transfer_seconds"],
            },
            "notes": [
                "Pulsar namespace 不切换：全局单池 1.0 定案，迁移对 Pulsar 侧为空操作",
                "源 EX keyspace 宽限期清理本演练 grace=0 即期执行（DROP 已校验无残留）",
            ],
            "known_gaps": [
                "API 层多 Cell 连接路由（M9 遗留，演练经 cell2/EX-2 直连客户端校验）",
            ],
        }
        with open(DRILL_RECORD, "a") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    finally:
        # 清理：迁移后图在 cell2、EX 在 EX-2——destroy 依赖指向目标侧客户端；
        # 源 EX keyspace 若因失败残留一并清掉（幂等）
        with contextlib.suppress(Exception):
            stack.ex_session.execute(f"DROP KEYSPACE IF EXISTS {ks}", timeout=120)
        with contextlib.suppress(Exception):
            stack.ex2_session.execute(f"DROP KEYSPACE IF EXISTS {ks}", timeout=120)
        with contextlib.suppress(SpaceNotFoundError):
            destroy_space(
                DestroyDeps(
                    store=stack.store,
                    gremlin=stack.cell2_gremlin,
                    cell_session=stack.cell2_session,
                    ex_session=stack.ex2_session,
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
