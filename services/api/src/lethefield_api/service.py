"""四操作业务核心（框架无关）：FastAPI / MCP / SDK 都是这层上面的薄壳。

边界（开发文档 §6 明确不做 + §9 定稿）：
- 任何接口不绕过 EX 直接写 RMS——`reinforce` 是唯一保留的同步直连旁路，
  且必须异步补 EX 元事件（fire-and-forget，§13.7；时间窗合并归 M7）。
- record / flag_conflict 只到 EX ack 为止；SS→RMS 异步入链（M14/M15）不在返回路径。
- FF 内部字段（s_effective、φ_i 计数器等）一律 `debug` scope 才出——四个操作统一适用，
  防"经 reinforce 等写接口旁路探测 FF 状态"。
- reinforce 不触发 consolidation worker：本模块不依赖 Pulsar/consolidation 任何入口
  （结构性保证，集成测试断言）。

约定：图名 = space_id（每 space 一图；M9 调度器落地后改查映射表）。
n_now 由 EX 摄入路径维护（Redis），读取走 `ex_ingest.n_now`——调用方从不传 n。
"""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client as GremlinClient
from lethefield_clients import es_client, ex_cassandra_cluster, gremlin_client, redis_client
from lethefield_rms import ff
from lethefield_rms.retrieve import RetrievalResult
from lethefield_rms.retrieve import retrieve as rms_retrieve
from redis import Redis

from lethefield_api import ex_ingest
from lethefield_api.auth import Claims, has_debug, require_scope, require_space

logger = logging.getLogger(__name__)

# 元事件追加器签名（fire-and-forget；测试可注入同步实现等待落库）
MetaAppender = Callable[..., None]


@dataclass
class ApiContext:
    """接口层依赖容器（构造时注入，测试逐项替换）。"""

    gremlin: GremlinClient
    es: Elasticsearch
    ex_session: Session
    redis: Redis
    meta_appender: MetaAppender

    @classmethod
    def from_env(cls) -> "ApiContext":
        ctx = cls.__new__(cls)
        ctx.gremlin = gremlin_client()
        ctx.es = es_client()
        ctx.ex_session = ex_cassandra_cluster().connect()
        ctx.redis = redis_client()
        ctx.meta_appender = _make_background_appender(ctx)
        return ctx


def _make_background_appender(ctx: ApiContext) -> MetaAppender:
    """默认追加器：后台 daemon 线程 fire-and-forget（调用方不等确认，§13.7）。

    失败不静默吞——记 warning 日志（无法回传调用方是 fire-and-forget 的固有语义）。
    """

    def append(**kwargs) -> None:
        def run() -> None:
            try:
                ex_ingest.append_meta(ctx.ex_session, **kwargs)
            except Exception:
                logger.warning("EX 元事件追加失败（fire-and-forget）: %s", kwargs, exc_info=True)

        threading.Thread(target=run, daemon=True).start()

    return append


def gname_of(space_id: str) -> str:
    """图名 = space_id（约定；M9 调度器落地后改查 ControlPlaneStore 映射）。"""
    return space_id


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
    event_id, n = ex_ingest.append_experience(
        ctx.ex_session,
        ctx.redis,
        space_id=space_id,
        content=content,
        agent_actor_id=claims.agent_actor_id,  # 盖章字段只认 claim
        account_id=claims.account_id,
        tau_ms=tau_ms,
    )
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
    event_id, n = ex_ingest.append_experience(
        ctx.ex_session,
        ctx.redis,
        space_id=space_id,
        content=content,
        agent_actor_id=claims.agent_actor_id,
        account_id=claims.account_id,
        tau_ms=tau_ms,
        ref_conflict=ref_conflict,
    )
    return {"event_id": event_id, "n": n, "space_id": space_id}


def reinforce(ctx: ApiContext, claims: Claims, *, space_id: str, node_key: str) -> dict:
    """memory.reinforce：唯一同步直连 RMS 的旁路（+0.2），同时异步补 EX 元事件。

    不经过 consolidation worker（结构性：本模块无 Pulsar/consolidation 依赖）。
    """
    require_scope(claims, "reinforce")
    require_space(claims, space_id)
    n_now = ex_ingest.n_now(ctx.redis, ctx.ex_session, space_id=space_id)
    state = ff.apply_reinforce(
        ctx.gremlin, gname_of(space_id), space_id=space_id, node_key=node_key, n_now=n_now
    )
    ctx.meta_appender(
        space_id=space_id,
        node_key=node_key,
        meta_type="reinforce",
        n_at_event=n_now,
        agent_actor_id=claims.agent_actor_id,
        account_id=claims.account_id,
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


def _present_node(ctx: ApiContext, space_id: str, node, *, debug: bool) -> dict:
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
            phi = ff.read_phi(
                ctx.gremlin, gname_of(space_id), space_id=space_id, node_key=node.node_key
            )
            item["phi"] = {
                "s": phi.s,
                "n_last_touched": phi.n_last_touched,
                "n_star_cached": phi.n_star_cached,
                "reinforce_count": phi.reinforce_count,
                "conflict_count": phi.conflict_count,
                "neglect_count": phi.neglect_count,
            }
    return item


def present(ctx: ApiContext, result: RetrievalResult, *, space_id: str, debug: bool) -> dict:
    """RetrievalResult → 对外响应（debug 裁剪规则的唯一实现点）。"""
    return {
        "nodes": [_present_node(ctx, space_id, n, debug=debug) for n in result.nodes],
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
    n_now = ex_ingest.n_now(ctx.redis, ctx.ex_session, space_id=space_id)
    result = rms_retrieve(
        ctx.gremlin,
        ctx.es,
        gname_of(space_id),
        space_id=space_id,
        query_text=query_text,
        query_vector=query_vector,
        n_now=n_now,
        rho=rho,
        trace_history=trace_history,
    )
    return present(ctx, result, space_id=space_id, debug=has_debug(claims))
