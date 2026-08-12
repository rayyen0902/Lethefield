"""四操作业务核心（框架无关）：FastAPI / MCP / SDK 都是这层上面的薄壳。

边界（开发文档 §6 明确不做 + §9 定稿）：
- 任何接口不绕过 EX 直接写 RMS——`reinforce` 是唯一保留的同步直连旁路，
  且必须异步补 EX 元事件（fire-and-forget，§13.7；时间窗合并在 ex_ingest，M7）。
- record / flag_conflict 只到 EX ack 为止；SS→RMS 异步入链（M14/M15）不在返回路径。
- FF 内部字段（s_effective、φ_i 计数器等）一律 `debug` scope 才出——四个操作统一适用，
  防"经 reinforce 等写接口旁路探测 FF 状态"。
- reinforce 不触发 consolidation worker：本模块不依赖 consolidation 任何入口
  （结构性保证，集成测试断言）。Pulsar 口径（M14 红线修订）：**同步返回路径不依赖
  Pulsar**——EX→Pulsar producer 显式登记在 `stream_publisher` 单点，发布在 EX ack
  之后、失败可容忍（page 告警 + 指标，不阻塞返回）。

约定：图名 = space_id（M5 冻结契约不变）；M9 起解析必经映射缓存（MappingCache
包装 MappingTableControlPlaneStore），未注册 space → 404——消除"绕过映射直连默认
集群"的快路径。计算侧持缓存直连 Cell，调度器/控制面宕机不影响存量读写（§17.2）。
n_now 由 EX 摄入路径维护（Redis），读取走 `ex_ingest.n_now`——调用方从不传 n。
"""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import uuid4

from cassandra.cluster import Cluster, Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client as GremlinClient
from lethefield_clients import (
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceNotFoundError,
    SpaceStatus,
    cassandra_cluster,
    es_client,
    ex_cassandra_cluster,
    gremlin_client,
    pulsar_client,
    redis_client,
    space_ref_of,
)
from lethefield_clients.spaces import validate_space_id
from lethefield_logschema import LogEvent
from lethefield_logschema import emit as emit_event
from lethefield_metrics import histogram
from lethefield_rms import ff
from lethefield_rms.retrieve import RetrievalResult
from lethefield_rms.retrieve import retrieve as rms_retrieve
from prometheus_client import REGISTRY
from redis import Redis

from lethefield_api import ex_ingest
from lethefield_api.auth import Claims, has_debug, require_scope, require_space
from lethefield_api.errors import ApiError, ErrorCode
from lethefield_api.stream_publisher import ExStreamPublisher

logger = logging.getLogger(__name__)

# 元事件追加器签名（fire-and-forget；测试可注入同步实现等待落库）
MetaAppender = Callable[..., None]

# 召回明细 query 类别枚举（M11 v1.2 定案最小化字段之一；按请求形态归类）
QUERY_CLASSES: frozenset[str] = frozenset({"keyword", "vector", "hybrid"})

# M12 埋点：record 确认延迟（§9.3 预算，同步等 EX ack 的全链路口径）
_RECORD_CONFIRM_DURATION = histogram(
    "lethefield_record_confirm_duration_seconds",
    "memory.record 确认延迟（含 EX 落库 ack）",
    registry=REGISTRY,
)


def query_class_of(query_text: str | None, query_vector: list[float] | None) -> str:
    """请求形态 → query 类别枚举（单点，禁放 query 原文进明细）。"""
    if query_text is not None and query_vector is not None:
        return "hybrid"
    if query_vector is not None:
        return "vector"
    return "keyword"


@dataclass
class ApiContext:
    """接口层依赖容器（构造时注入，测试逐项替换）。"""

    gremlin: GremlinClient
    es: Elasticsearch
    ex_session: Session
    redis: Redis
    meta_appender: MetaAppender
    mapping_cache: MappingCache
    control_cluster: Cluster | None = None  # 控制面独立连接（故障演练可与数据面隔离）
    stream_publisher: ExStreamPublisher | None = None  # M14 EX→Pulsar 生产侧（None=不发布）

    @classmethod
    def from_env(cls) -> "ApiContext":
        ctx = cls.__new__(cls)
        ctx.gremlin = gremlin_client()
        ctx.es = es_client()
        ctx.ex_session = ex_cassandra_cluster().connect()
        ctx.redis = redis_client()
        ctx.meta_appender = _make_background_appender(ctx)
        # M14：EX→Pulsar 生产侧（落库确认后发布；失败不阻塞同步返回）
        ctx.stream_publisher = ExStreamPublisher(pulsar_client())
        # 控制面独立 Cluster 连接：调度器/元数据故障面与图/EX 访问隔离（M9 演练前提）
        ctx.control_cluster = cassandra_cluster()
        store = MappingTableControlPlaneStore(ctx.control_cluster.connect())
        store.ensure_tables()
        ctx.mapping_cache = MappingCache(store)
        return ctx


def _make_background_appender(ctx: ApiContext) -> MetaAppender:
    """默认追加器：后台 daemon 线程 fire-and-forget（调用方不等确认，§13.7）。

    失败不静默吞——记 warning 日志（无法回传调用方是 fire-and-forget 的固有语义）。
    """

    def append(**kwargs) -> None:
        def run() -> None:
            try:
                ex_ingest.append_meta(ctx.ex_session, redis=ctx.redis, **kwargs)
            except Exception:
                logger.warning("EX 元事件追加失败（fire-and-forget）: %s", kwargs, exc_info=True)

        threading.Thread(target=run, daemon=True).start()

    return append


def _resolve_gname(ctx: ApiContext, space_id: str) -> str:
    """space → 图名解析（M9）：必经映射缓存，未注册 space 404 fail-closed。

    图名约定 = space_id 不变；解析的意义是"该 space 已开通且归属已知 Cell"——
    多 Cell 形态下映射的 cell_id 决定连接路由，本地单 Cell 形态下是存在性闸门。
    """
    try:
        return ctx.mapping_cache.get_space_mapping(space_id).space_id
    except SpaceNotFoundError:
        raise ApiError(ErrorCode.NOT_FOUND, f"space {space_id} 未开通或已注销") from None


def _require_valid_space(space_id: str) -> None:
    """space_id 字符集 fail-closed（M8）：非法值 400，不直达图名/keyspace 命名路径。"""
    try:
        validate_space_id(space_id)
    except ValueError as exc:
        raise ApiError(ErrorCode.BAD_REQUEST, str(exc)) from None


def _require_not_migrating(ctx: ApiContext, space_id: str) -> None:
    """迁移窗口内写操作明确失败（M10，设计文档 §18.3：生产者收到明确错误并重试缓冲）。

    复用 429 rate_limited 契约码（"稍后重试"语义；新增错误码属契约变更，不走）。
    retrieve 只读不受影响——迁移是只读窗口，读在源 Cell 持续服务到清理前。
    """
    if ctx.mapping_cache.get_space_mapping(space_id).status is SpaceStatus.MIGRATING:
        raise ApiError(ErrorCode.RATE_LIMITED, f"space {space_id} 迁移中，写入暂停，请缓冲重试")


def record(
    ctx: ApiContext,
    claims: Claims,
    *,
    space_id: str,
    content: str,
    tau_ms: int | None = None,
) -> dict:
    """memory.record：转发 EX 摄入，同步等 EX ack 后返回；不直接操作 RMS 图。"""
    require_scope(claims, "record")
    require_space(claims, space_id)
    _require_valid_space(space_id)
    _resolve_gname(ctx, space_id)  # 未开通 space fail-closed（M9）
    _require_not_migrating(ctx, space_id)  # M10：迁移窗口写入明确失败
    t0 = time.perf_counter()
    event_id, n = ex_ingest.append_experience(
        ctx.ex_session,
        ctx.redis,
        space_id=space_id,
        content=content,
        agent_actor_id=claims.agent_actor_id,  # 盖章字段只认 claim
        account_id=claims.account_id,
        tau_ms=tau_ms,
        publisher=ctx.stream_publisher,  # M14：EX ack 后发布 ex-events（失败不阻塞返回）
    )
    _RECORD_CONFIRM_DURATION.observe(time.perf_counter() - t0)  # M12：§9.3 确认延迟
    return {"event_id": event_id, "n": n, "space_id": space_id}


def flag_conflict(
    ctx: ApiContext,
    claims: Claims,
    *,
    space_id: str,
    content: str,
    ref_conflict: str,
    tau_ms: int | None = None,
) -> dict:
    """memory.flag_conflict：纠错 = 携带被纠正引用的普通经验事件，走正常入链。"""
    require_scope(claims, "flag_conflict")
    require_space(claims, space_id)
    _require_valid_space(space_id)
    _resolve_gname(ctx, space_id)  # 未开通 space fail-closed（M9）
    _require_not_migrating(ctx, space_id)  # M10：迁移窗口写入明确失败
    event_id, n = ex_ingest.append_experience(
        ctx.ex_session,
        ctx.redis,
        space_id=space_id,
        content=content,
        agent_actor_id=claims.agent_actor_id,
        account_id=claims.account_id,
        tau_ms=tau_ms,
        ref_conflict=ref_conflict,
        publisher=ctx.stream_publisher,  # M14：EX ack 后发布 ex-events（失败不阻塞返回）
    )
    return {"event_id": event_id, "n": n, "space_id": space_id}


def reinforce(ctx: ApiContext, claims: Claims, *, space_id: str, node_key: str) -> dict:
    """memory.reinforce：唯一同步直连 RMS 的旁路（+0.2），同时异步补 EX 元事件。

    不经过 consolidation worker（结构性：本模块无 consolidation 依赖；Pulsar 生产侧
    登记在 stream_publisher 单点，只服务 EX 事件流发布，与 reinforce 旁路无关）。
    """
    require_scope(claims, "reinforce")
    require_space(claims, space_id)
    _require_valid_space(space_id)
    gname = _resolve_gname(ctx, space_id)
    _require_not_migrating(ctx, space_id)  # M10：迁移窗口写入明确失败（含 RMS 旁路）
    n_now = ex_ingest.n_now(ctx.redis, ctx.ex_session, space_id=space_id)
    state = ff.apply_reinforce(
        ctx.gremlin, gname, space_id=space_id, node_key=node_key, n_now=n_now
    )
    ctx.meta_appender(
        space_id=space_id,
        node_key=node_key,
        meta_type="reinforce",
        n_at_event=n_now,
        agent_actor_id=claims.agent_actor_id,
        account_id=claims.account_id,
        merge_window_ms=ex_ingest.REINFORCE_MERGE_WINDOW_MS,  # M7 时间窗合并
    )
    # M12：reinforce 明细事件——ff_recalled_then_touched_rate 的 touched 侧数据源
    # （离线聚合从日志管线关联召回明细，不扫业务库）。
    emit_event(
        LogEvent(
            service="lethefield-api",
            event_type="memory_reinforced",
            space_id=space_id,
            payload={"node_key": node_key, "n_now": n_now},
        )
    )
    result: dict = {"node_key": node_key, "applied": True}
    if has_debug(claims):
        result["phi"] = {
            "s": state.s,
            "n_last_touched": state.n_last_touched,
            "n_star_cached": state.n_star_cached,
            "reinforce_count": state.reinforce_count,
        }
    return result


def _present_node(ctx: ApiContext, gname: str, space_id: str, node, *, debug: bool) -> dict:
    """响应节点裁剪：默认只给内容+关系+brief；debug scope 附 φ_i 内部字段。

    debug 是低频诊断路径，φ_i 快照按节点 read_phi（N 次图往返可接受）。
    """
    item = {
        "node_key": node.node_key,
        "content": node.content,
        "tau": node.tau.isoformat() if node.tau else None,
        "brief": node.brief,
    }
    if debug:
        item["s_effective"] = node.s_effective
        item["relevance"] = node.relevance
        if node.s_effective is not None:  # 实体叶子无 φ
            phi = ff.read_phi(ctx.gremlin, gname, space_id=space_id, node_key=node.node_key)
            item["phi"] = {
                "s": phi.s,
                "n_last_touched": phi.n_last_touched,
                "n_star_cached": phi.n_star_cached,
                "reinforce_count": phi.reinforce_count,
                "conflict_count": phi.conflict_count,
                "neglect_count": phi.neglect_count,
            }
    return item


def present(
    ctx: ApiContext, result: RetrievalResult, *, gname: str, space_id: str, debug: bool
) -> dict:
    """RetrievalResult → 对外响应（debug 裁剪规则的唯一实现点）。"""
    return {
        "nodes": [_present_node(ctx, gname, space_id, n, debug=debug) for n in result.nodes],
        "edges": [
            {"out_key": e.out_key, "in_key": e.in_key, "label": e.label} for e in result.edges
        ],
    }


def retrieve(
    ctx: ApiContext,
    claims: Claims,
    *,
    space_id: str,
    query_text: str | None = None,
    query_vector: list[float] | None = None,
    rho: float = 1.0,
    trace_history: bool = False,
) -> dict:
    """memory.retrieve：M4 四阶段检索的外部入口（只读）；FF 字段按 debug scope 裁剪。"""
    require_scope(claims, "retrieve")
    require_space(claims, space_id)
    _require_valid_space(space_id)
    gname = _resolve_gname(ctx, space_id)
    n_now = ex_ingest.n_now(ctx.redis, ctx.ex_session, space_id=space_id)
    result = rms_retrieve(
        ctx.gremlin,
        ctx.es,
        gname,
        space_id=space_id,
        query_text=query_text,
        query_vector=query_vector,
        n_now=n_now,
        rho=rho,
        trace_history=trace_history,
    )
    _emit_recall_detail(
        space_id=space_id, result=result, query_text=query_text, query_vector=query_vector
    )
    return present(ctx, result, gname=gname, space_id=space_id, debug=has_debug(claims))


def _emit_recall_detail(
    *,
    space_id: str,
    result: RetrievalResult,
    query_text: str | None,
    query_vector: list[float] | None,
) -> None:
    """M11/M12 ③ 入料口数据源：召回明细日志事件（只进运维日志管线 es-ops）。

    字段最小化（v1.2 定案）：space_ref 不透明哈希 + 召回 node_key 列表 + θ 阶段计数
    + query 类别枚举；不含 query 原文与内容摘要（基数纪律）。M12 增补 event_id
    （at-least-once 去重键）与 stage_ms（Stage 耗时，lru 缓存失效代理指标原料）。

    训练 topic 的授权闸门在 recall_filter（读日志管线、入 topic 前查注册表）——
    本路径不直发训练 topic（M12 收口定案：物理分开，职责不塞请求路径服务）。
    """
    emit_event(
        LogEvent(
            service="lethefield-api",
            event_type="retrieve_recall_detail",
            space_id=space_id,
            payload={
                "event_id": uuid4().hex,
                "space_ref": space_ref_of(space_id),
                "node_keys": [n.node_key for n in result.nodes],
                "theta": result.stats,
                "query_class": query_class_of(query_text, query_vector),
                "stage_ms": {k: round(v * 1000, 3) for k, v in result.timings.items()},
            },
        )
    )
