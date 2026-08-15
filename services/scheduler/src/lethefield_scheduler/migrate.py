"""跨 Cell 迁移流水线（M10，设计文档 §18.3）。

步骤（严格按序）：标 migrating（写路径 429 起，**只读窗口起点**）→
RMS 目标侧重建（目标 Cell 建图+schema → M7 EX 重放 rebuild → ES 向量复制）→
EX keyspace 复制（snapshot → load → 校验）→ 校验三存储等价 → 切映射 +
status 回 active（**只读窗口终点**，实测时长入报告）→ 宽限期 → 源侧清理
（驱逐计算实例先于 DROP，红线 5）。

形态说明：
- **本地档**：RMS 真跨 Cell（源/目标 JG/Cassandra/ES 各自独立）；EX 同集群
  keyspace 复制演练——scratch keyspace 过渡（copy → 校验 → DROP 源 → 回拷正名 →
  DROP scratch），全步骤真实执行。keyspace 名 = `ex_{space_id}` 是冻结命名契约，
  同集群内"迁到目标集群的同名 keyspace"只能靠过渡拷贝达成最终正名状态。
- **准出档**：EX 跨集群迁移经 `ex_transfer` 注入点（sstableloader 流式传输的
  编排属演练工具链，不进本模块）+ `to_ex_cluster_id` 切映射时一并更新 EX 归属。
  两者必须同给或同不给（给了目标集群不做传输 = 映射指向无数据集群）。
  跨集群时源 EX keyspace 的宽限期清理按注销流程由调用方处置（设计 §11 迁移第 5 步）。
- Pulsar 全局单池 1.0 是空操作（namespace 不变）；粗粒度分片池演进后才有
  Pulsar 侧迁移（§18.2），此处留扩展点。

失败语义：切映射前失败 → 回滚目标侧半成品 + status 回 active（源仍服务）；
切映射后源侧清理失败 → 抛 MigrationCleanupError 携带残留清单（不静默）。
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients import (
    EXPERIENCE_TABLE,
    META_TABLE,
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceStatus,
    ensure_ex_keyspace_named,
    keyspace_name,
    validate_space_id,
)
from lethefield_rms import rebuild
from lethefield_rms.schema import backend_props_of, ensure_graph_schema
from lethefield_rms.vectors import VECTORS_INDEX, ensure_vectors_index

from lethefield_scheduler.config import DDL_TIMEOUT_SECONDS as _DDL_TIMEOUT_SECONDS
from lethefield_scheduler.config import DEFAULT_CONFIG, SchedulerConfig
from lethefield_scheduler.watermark import select_cell

logger = logging.getLogger(__name__)


class MigrationError(RuntimeError):
    """迁移失败且已回滚（或回滚也有残留——异常信息携带现场，不静默）。"""


class MigrationCleanupError(RuntimeError):
    """切映射后源侧清理发现残留（目标已服务，残留清单需人工处理）。"""


@dataclass
class MigrationReport:
    """迁移演练记录（只读窗口实测是 M10 验收硬指标）。"""

    space_id: str
    source_cell_id: str
    target_cell_id: str
    read_only_window_seconds: float
    ex_experience_rows: int
    ex_meta_rows: int
    rms_vertices: int
    rms_edges: int
    vector_docs: int
    step_seconds: dict[str, float] = field(default_factory=dict)


@dataclass
class MigrateDeps:
    """迁移流程依赖容器（构造时注入，测试逐项替换为 fake）。

    源/目标客户端分离是跨 Cell 的物理表达；本地档同集群 EX 共用一个 session。
    """

    store: MappingTableControlPlaneStore
    source_gremlin: Client
    target_gremlin: Client
    source_cell_session: Session
    target_cell_session: Session
    source_es: Elasticsearch
    target_es: Elasticsearch
    ex_session: Session
    config: SchedulerConfig = DEFAULT_CONFIG


_EXPERIENCE_COLUMNS = (
    "n, event_id, content, agent_actor_id, account_id, tau_ms, ref_conflict, created_at"
)
_META_COLUMNS = (
    "node_key, created_at, event_id, meta_type, count, n_at_event, agent_actor_id, "
    "account_id, details"  # details：M14 契约 1 演进（可空加列）
)


def _copy_ex_keyspace(
    source: Session, target: Session, source_ks: str, target_ks: str
) -> tuple[int, int]:
    """EX keyspace 全量复制（两表逐行拷贝），返回 (经验事件数, 元事件数)。

    应用层拷贝对应本地档"快照/导入"的最小诚实形态；准出档跨集群走
    sstableloader 流式传输（目标端连接/网络/认证是已登记缺口），接口不变。
    """
    ensure_ex_keyspace_named(target, target_ks)
    rows = source.execute(f"SELECT {_EXPERIENCE_COLUMNS} FROM {source_ks}.{EXPERIENCE_TABLE}").all()
    for row in rows:
        target.execute(
            f"INSERT INTO {target_ks}.{EXPERIENCE_TABLE} ({_EXPERIENCE_COLUMNS}) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(getattr(row, c) for c in _EXPERIENCE_COLUMNS.split(", ")),
        )
    metas = source.execute(f"SELECT {_META_COLUMNS} FROM {source_ks}.{META_TABLE}").all()
    for row in metas:
        target.execute(
            f"INSERT INTO {target_ks}.{META_TABLE} ({_META_COLUMNS}) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(getattr(row, c) for c in _META_COLUMNS.split(", ")),
        )
    return len(rows), len(metas)


def _count_ex_rows(session: Session, ks: str) -> tuple[int, int]:
    experience = session.execute(f"SELECT COUNT(*) AS c FROM {ks}.{EXPERIENCE_TABLE}").one().c
    meta = session.execute(f"SELECT COUNT(*) AS c FROM {ks}.{META_TABLE}").one().c
    return experience, meta


def _migrate_ex_same_cluster(session: Session, space_id: str) -> tuple[int, int]:
    """本地档 EX 迁移：同集群 scratch 过渡，全步骤真实执行（见模块 docstring）。

    copy 正名 → scratch（"目标集群"替身）→ 校验 → DROP 源 → 回拷正名 → 校验 →
    DROP scratch。中途失败由调用方回滚（scratch 尚存时可恢复正名）。
    """
    canonical = keyspace_name(space_id)
    scratch = f"{canonical}_mig"
    session.execute(f"DROP KEYSPACE IF EXISTS {scratch}", timeout=_DDL_TIMEOUT_SECONDS)
    counts = _copy_ex_keyspace(session, session, canonical, scratch)
    if _count_ex_rows(session, scratch) != counts:
        raise MigrationError(f"EX scratch 校验失败：{scratch} 行数不符 {counts}")
    session.execute(f"DROP KEYSPACE IF EXISTS {canonical}", timeout=_DDL_TIMEOUT_SECONDS)
    try:
        _copy_ex_keyspace(session, session, scratch, canonical)
    except Exception:
        # 正名回拷失败：scratch 仍在，尽力恢复原状
        _copy_ex_keyspace(session, session, scratch, canonical)
        raise
    if _count_ex_rows(session, canonical) != counts:
        raise MigrationError(f"EX 回拷校验失败：{canonical} 行数不符 {counts}")
    session.execute(f"DROP KEYSPACE IF EXISTS {scratch}", timeout=_DDL_TIMEOUT_SECONDS)
    return counts


def _graph_counts(gremlin: Client, gname: str) -> tuple[int, int]:
    """图顶点/边计数（迁移等价性校验用）。"""
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


def _copy_vectors(source_es: Elasticsearch, target_es: Elasticsearch, space_id: str) -> int:
    """rms_vectors 文档应用层复制：源 ES 按 space_id+routing 查 → 目标 ES 同 routing 写回。

    1.0 演练规模单批查全量（演练数据量 << 10k）；生产规模走 scroll/pit，留扩展点。
    """
    mapping = source_es.indices.get_mapping(index=VECTORS_INDEX)
    dims = mapping.body[VECTORS_INDEX]["mappings"]["properties"]["v"]["dims"]
    ensure_vectors_index(target_es, index=VECTORS_INDEX, dims=int(dims))
    hits = source_es.search(
        index=VECTORS_INDEX,
        query={"term": {"space_id": space_id}},
        routing=space_id,
        size=10_000,
    ).body["hits"]["hits"]
    for hit in hits:
        target_es.index(
            index=VECTORS_INDEX,
            id=hit["_id"],
            document=hit["_source"],
            routing=space_id,
            refresh=True,
        )
    return len(hits)


def _evict_graph(gremlin: Client, gname: str) -> None:
    """驱逐计算实例（红线 5 顺序，与 destroy 同语义：close 容错、removeConfiguration 严格）。"""
    gremlin.submit(
        "if (ConfiguredGraphFactory.getGraphNames().contains(gname)) { "
        "try { ConfiguredGraphFactory.close(gname) } catch (Exception ignored) {}; "
        "ConfiguredGraphFactory.removeConfiguration(gname) "
        "}; 'evicted'",
        {"gname": gname},
    ).all().result()


def _drop_graph_storage(
    cell_session: Session, es: Elasticsearch, gname: str, space_id: str
) -> None:
    cell_session.execute(f"DROP KEYSPACE IF EXISTS {gname}", timeout=_DDL_TIMEOUT_SECONDS)
    es.options(ignore_status=(404,)).delete_by_query(
        index=VECTORS_INDEX,
        query={"term": {"space_id": space_id}},
        routing=space_id,
        conflicts="proceed",
        refresh=True,
    )


def migrate_space(
    deps: MigrateDeps,
    space_id: str,
    *,
    to_cell_id: str | None = None,
    cache: MappingCache | None = None,
    grace_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    to_ex_cluster_id: str | None = None,
    ex_transfer: Callable[[], tuple[int, int]] | None = None,
) -> MigrationReport:
    """迁移一个 space 到目标 Cell（端到端；只读窗口实测入报告）。

    准出档跨集群 EX：`ex_transfer` 注入 EX 迁移步骤（返回 (经验事件数, 元事件数)，
    内部须含传输后校验），`to_ex_cluster_id` 在切映射时替换映射的 EX 归属；
    两者必须成对提供（默认 None = 本地档同集群语义）。
    """
    validate_space_id(space_id)
    if (to_ex_cluster_id is None) != (ex_transfer is None):
        raise MigrationError("to_ex_cluster_id 与 ex_transfer 必须成对提供（跨集群 EX 迁移语义）")
    mapping = deps.store.get_space_mapping(space_id)  # 未注册 fail-closed
    if mapping.status is not SpaceStatus.ACTIVE:
        raise MigrationError(f"space {space_id} 状态 {mapping.status} 不可迁移（需 active）")
    gname = space_id  # 图名 = space_id（M5 冻结契约）

    if to_cell_id is not None:
        target_cell = deps.store.get_cell(to_cell_id)
        if target_cell.cell_id == mapping.cell_id:
            raise MigrationError(f"目标 Cell 与源相同（{to_cell_id}），迁到自己不算迁移")
    else:
        target_cell = select_cell(deps.store, exclude=frozenset({mapping.cell_id}))

    steps: dict[str, float] = {}

    def mark(step: str, since: float) -> float:
        steps[step] = round(clock() - since, 3)
        return clock()

    window_start = clock()
    # 1. 标 migrating（写路径 429 起；读取仍走源 Cell）——只读窗口起点
    deps.store.update_space_status(space_id, SpaceStatus.MIGRATING)
    if cache is not None:
        cache.invalidate(space_id)
    t = clock()

    scratch = f"{keyspace_name(space_id)}_mig"
    cutover_done = False
    try:
        # 2. RMS 目标侧重建：目标 Cell 建图+schema（backend props 按目标 Cell endpoints
        #    推导）→ M7 EX 重放（复用已验证重建链，§13.7 可重建性即迁移能力）
        ensure_graph_schema(
            deps.target_gremlin, gname, backend_props=backend_props_of(target_cell.endpoints)
        )
        plan = rebuild.rebuild_space(
            deps.target_gremlin,
            deps.target_cell_session,
            deps.ex_session,
            space_id=space_id,
            target_gname=gname,
            # M13 红线 3：归档 v_i lookup 走源侧——旧 archived_nodes 在源 Cell，
            # rms_vectors 文档在源 ES（向量复制在下一步才发生）
            es=deps.source_es,
            source_cell_session=deps.source_cell_session,
        )
        t = mark("rms_rebuild", t)

        # 3. ES 向量复制（rebuild 不覆盖向量，独立复制）
        vector_docs = _copy_vectors(deps.source_es, deps.target_es, space_id)
        t = mark("vector_copy", t)

        # 4. EX keyspace 复制（本地档同集群 scratch 过渡 / 准出档跨集群注入传输，
        #    全步骤真实执行）
        if ex_transfer is not None:
            ex_experience, ex_meta = ex_transfer()
        else:
            ex_experience, ex_meta = _migrate_ex_same_cluster(deps.ex_session, space_id)
        t = mark("ex_copy", t)

        # 5. 等价校验：目标图 == EX 重放计划（不是与源图比——源图可能含重放不覆盖的
        #    consolidation 推断边（M7 既定语义），且窗口翻转前在途的合法写入只反映在
        #    重放里；EX 是唯一 source of truth）
        live_keys = {n.node_key for n in plan.nodes}
        expected_v = len(plan.nodes)
        expected_e = sum(
            1
            for a, b in (*plan.temporal_edges, *plan.supersedes_edges)
            if a in live_keys and b in live_keys
        )
        target_v, target_e = _graph_counts(deps.target_gremlin, gname)
        if (target_v, target_e) != (expected_v, expected_e):
            raise MigrationError(
                f"RMS 等价校验失败：目标 ({target_v}v/{target_e}e) ≠ EX 重放计划 "
                f"({expected_v}v/{expected_e}e)"
            )
        t = mark("verify", t)

        # 6. 切映射 + status 回 active——只读窗口终点。
        #    不归点 = update_space_cell（映射指向目标）：此后失败不得回滚目标侧，
        #    空间已由目标 Cell 服务，残留按清理异常上交。
        deps.store.update_space_cell(
            space_id, target_cell.cell_id, to_ex_cluster_id or mapping.ex_cluster_id
        )
        cutover_done = True
        if cache is not None:
            cache.invalidate(space_id)
        deps.store.update_space_status(space_id, SpaceStatus.ACTIVE)
        window_seconds = clock() - window_start
        t = mark("cutover", t)
    except Exception as exc:
        if not cutover_done:
            rollback_errors = _rollback(deps, space_id, gname, scratch)
            deps.store.update_space_status(space_id, SpaceStatus.ACTIVE)
            if cache is not None:
                cache.invalidate(space_id)
            detail = f"迁移 {space_id} 失败，已回滚（源仍服务）：{exc}"
            if rollback_errors:
                detail += "；回滚残留：" + ", ".join(f"{n}: {e}" for n, e in rollback_errors)
            raise MigrationError(detail) from exc
        raise MigrationCleanupError(
            f"space {space_id} 已切映射到 {target_cell.cell_id}（目标在服务），"
            f"但切映射收尾失败（状态可能仍 migrating，需人工核）：{exc}"
        ) from exc

    # 7. 宽限期后源侧清理（红线 5：驱逐先于 DROP）；清理残留不静默
    if grace_seconds > 0:
        sleep(grace_seconds)
    residues: list[str] = []
    for name, action in (
        ("evict_source_graph", lambda: _evict_graph(deps.source_gremlin, gname)),
        (
            "drop_source_storage",
            lambda: _drop_graph_storage(deps.source_cell_session, deps.source_es, gname, space_id),
        ),
    ):
        try:
            action()
        except Exception as exc:  # noqa: BLE001 — 残留收进清单，继续清下一项
            residues.append(f"{name}: {exc}")
    if residues:
        raise MigrationCleanupError(
            f"space {space_id} 已切到 {target_cell.cell_id} 并在服务，源侧清理残留："
            + "; ".join(residues)
        )
    mark("source_cleanup", t)

    return MigrationReport(
        space_id=space_id,
        source_cell_id=mapping.cell_id,
        target_cell_id=target_cell.cell_id,
        read_only_window_seconds=round(window_seconds, 3),
        ex_experience_rows=ex_experience,
        ex_meta_rows=ex_meta,
        rms_vertices=target_v,
        rms_edges=target_e,
        vector_docs=vector_docs,
        step_seconds=steps,
    )


def _rollback(
    deps: MigrateDeps, space_id: str, gname: str, scratch: str
) -> list[tuple[str, Exception]]:
    """切映射前失败的回滚（尽力而为）：目标图+目标 ES 文档+EX scratch 清掉，
    EX 正名缺失时从 scratch 恢复。失败收进清单（不静默）。"""
    errors: list[tuple[str, Exception]] = []
    canonical = keyspace_name(space_id)
    actions: list[tuple[str, Callable[[], None]]] = [
        ("drop_target_graph", lambda: _evict_graph(deps.target_gremlin, gname)),
        (
            "drop_target_storage",
            lambda: _drop_graph_storage(deps.target_cell_session, deps.target_es, gname, space_id),
        ),
        (
            "restore_ex_canonical",
            lambda: (
                _copy_ex_keyspace(deps.ex_session, deps.ex_session, scratch, canonical)
                if not _keyspace_exists(deps.ex_session, canonical)
                else None
            ),
        ),
        (
            "drop_ex_scratch",
            lambda: deps.ex_session.execute(
                f"DROP KEYSPACE IF EXISTS {scratch}", timeout=_DDL_TIMEOUT_SECONDS
            ),
        ),
    ]
    for name, action in actions:
        try:
            action()
        except Exception as exc:  # noqa: BLE001 — 回滚尽力而为，异常收进清单
            errors.append((name, exc))
    return errors


def _keyspace_exists(session: Session, ks: str) -> bool:
    return (
        session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", (ks,)
        ).one()
        is not None
    )
