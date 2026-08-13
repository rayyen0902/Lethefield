"""建点编排（M15，v1.2 修订记录第 23 条定案）。

幂等三分解（at-least-once 消费下重复零副作用，覆盖部分失败补全）：
- 顶点：vertex_exists(node_key) 预检，缺失才 create_event_node；
- 时序边：图内前驱存在则 temporal_edge_exists 预检，缺失才 create_edge；
- 向量：get_vector 预检，缺失才 embed + index_vector（doc id 覆盖写兜底）。
三项都在才判 duplicate——"建点成功但崩在建边/向量前"的重试走补全路径。

字段来源（第 23 条①）：ScoringResult 信封只作触发 + s/node_key 来源；
c_i/τ_i/A_i 按 n 反查 EX——A_i 取自 experience_events.agent_actor_id 列
（摄入层按 JWT claim 盖章，禁从事件体自由文本读）。
"""

from datetime import UTC, datetime

from lethefield_clients.ex_n import get_experience_event
from lethefield_rms import vectors
from lethefield_rms import writer as rms_writer
from lethefield_rms.rebuild import node_key_of

from lethefield_writer.metrics import EMBED_CALLS_TOTAL, EMBED_TOKENS_TOTAL


class ExEventMissing(Exception):
    """EX 查无该 n 的经验事件（上游不一致：SS 打分源事件必然先落 EX）。"""


def _epoch_ms(dt: datetime) -> int:
    """Cassandra timestamp → epoch 毫秒（naive 按 UTC 解释，M6 踩坑定案）。"""
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return int(aware.timestamp() * 1000)


def ensure_node(deps, *, space_id: str, event_id: str, node_key: str, n: int, s: float) -> str:
    """建点幂等编排：返回 "created"（新建或部分补全）/ "duplicate"（三分解全在零写入）。

    deps 协议：gremlin / es / ex_session / embedder / quota_counters（WorkerDeps 同款）。
    图名 = space_id（M5 定案）。一致性校验 fail-closed：node_key 必须等于
    node_key_of(event_id)（第 23 条②），且 EX 行的 event_id 必须与入参一致。
    时序边前序 = 图内 n_created < n 的最大者（第 23 条③：归档缺口不跨接）。
    异常上抛（由运行时 nack → 重投 → DLQ）。
    """
    if node_key != node_key_of(event_id):
        raise ValueError(
            f"node_key 与 event_id 不符：{node_key!r} vs {node_key_of(event_id)!r}（fail-closed）"
        )
    gname = space_id
    vertex_ok = rms_writer.vertex_exists(deps.gremlin, gname, space_id=space_id, node_key=node_key)
    pred = rms_writer.latest_event_node(deps.gremlin, gname, space_id=space_id, before_n=n)
    edge_ok = pred is None or rms_writer.temporal_edge_exists(
        deps.gremlin, gname, space_id=space_id, from_key=pred[0], to_key=node_key
    )
    vector_ok = vectors.get_vector(deps.es, space_id=space_id, node_key=node_key) is not None
    if vertex_ok and edge_ok and vector_ok:
        return "duplicate"

    event = get_experience_event(deps.ex_session, space_id=space_id, n=n)
    if event is None:
        raise ExEventMissing(f"EX 查无经验事件：space={space_id} n={n}（上游不一致）")
    if event.event_id != event_id:
        raise ValueError(
            f"信封 event_id 与 EX 行不符：{event_id!r} vs {event.event_id!r}（fail-closed）"
        )
    if not vertex_ok:
        rms_writer.create_event_node(
            deps.gremlin,
            gname,
            node_key=node_key,
            space_id=space_id,
            content=event.content,
            tau_ms=event.tau_ms if event.tau_ms is not None else _epoch_ms(event.created_at),
            ref_ex=event_id,
            s=s,
            n_created=n,
            agent_actor_id=event.agent_actor_id,
            quota_counters=deps.quota_counters,
        )
    if not edge_ok:
        rms_writer.create_edge(
            deps.gremlin,
            gname,
            space_id=space_id,
            from_key=pred[0],
            to_key=node_key,
            label="temporal",
            quota_counters=deps.quota_counters,
        )
    if not vector_ok:
        try:
            vector, usage = deps.embedder.embed(event.content)
        except Exception:
            EMBED_CALLS_TOTAL.labels(result="failed").inc()
            raise
        EMBED_CALLS_TOTAL.labels(result="ok").inc()
        EMBED_TOKENS_TOTAL.labels(type="prompt").inc(usage.get("prompt_tokens", 0))
        EMBED_TOKENS_TOTAL.labels(type="total").inc(usage.get("total_tokens", 0))
        vectors.index_vector(
            deps.es,
            space_id=space_id,
            node_key=node_key,
            vector=vector,
            content=event.content,
            quota_counters=deps.quota_counters,
        )
    return "created"
