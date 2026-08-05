"""四阶段检索（开发文档 §5，M4）：召回单元是带边子图，不是孤立节点列表。

阶段纪律（不可合并/跳过）：
- Stage 1（隐式）：`space_id` 必填，是检索范围硬边界；禁止跨 space 全局检索。
- Stage 2 锚点识别（ES）：kNN + 关键词两路，RRF 融合（**s 永不进 RRF**）；
  候选集产出后做**第一次 θ_effective 硬过滤**（现算 s_effective，低于即弃）。
- Stage 3 自适应遍历（JanusGraph）：`_stage3_traverse` **签名无 es、无 rho**——
  "Stage 3 不访问 ES" 与 "ρ 只作用于两处硬过滤、不影响 λ3 软惩罚" 两条约束
  由函数签名物理隔离，不靠自觉。束搜索收敛后的**第二次独立硬过滤**在 retrieve()
  主函数执行（θ 需要 ρ，但不能因此让 ρ 渗入遍历）。
- supersedes（Stage 3 内嵌）：带 supersedes 入边的节点默认沿链重定向至最新取代者，
  被取代节点不进候选池；`trace_history=True` 时保留（显式追溯历史）。
- Stage 4 token 预算（应用层）：relevance 与 s_effective 双权重，两项都高 → 完整
  内容；相关但 s_effective 低 → 压缩为简短提示（截断占位，正式压缩语义待标定）。

前置粗筛：图查询一律 `has('n_star_cached', gt(n_now))`（M3 绝对视界），排除明显
跨越遗忘视界的节点；粗筛只用于降开销，不参与实时计算。

固化节点（M6，带 `consolidated_at`）：s 锁定——s_effective 取 s 现值、跳过衰减
现算；两处 θ_effective 硬过滤对其跳过丢弃判定；固化时 n_star_cached 已置
LONG_MAX，粗筛天然放行。

本轮升级确认的定案（见工作日志 M4）：
- λ1·φ = 边类型先验权重 `edge_prior[label]`（占位常数，标定流程调整）；
- λ2·sim 方案 A：锚点 = Stage 2 RRF 分，扩展节点 sim=0（**方案 B——继承父节点
  sim 衰减——留待效果验证后探索**，届时改 `_beam_search` 一处即可）；
- 实体顶点无 φ（不参与 FF 衰减），不作束搜索扩展路径；实体关系在收敛后统一
  以叶子节点 + entity 边形式挂回结果子图。
"""

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime

from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client

from lethefield_rms import ff
from lethefield_rms.vectors import keyword_search, knn_search

# ---------------------------------------------------------------- 配置与数据结构


@dataclass(frozen=True)
class RetrieveConfig:
    """检索参数（全为 v0.1 占位，调整走参数标定流程，不是随意改代码）。"""

    rrf_k: int = 60  # RRF 融合常数
    knn_k: int = 10  # Stage 2 kNN 一路召回数
    keyword_k: int = 10  # Stage 2 关键词一路召回数
    anchor_top_k: int = 10  # RRF 融合后保留的锚点数
    beam_width: int = 8  # 束搜索每轮扩展节点数
    max_depth: int = 2  # 最大扩展深度（对齐 spike q1 两跳）
    # λ1·φ 的边类型先验（实体边不作扩展路径，见模块 docstring）
    edge_prior: dict[str, float] = field(
        default_factory=lambda: {"causal": 1.0, "semantic": 0.8, "temporal": 0.6}
    )
    lambda1: float = 1.0  # 结构先验权重
    lambda2: float = 1.0  # query 相似度权重（扩展节点 sim=0，方案 A）
    lambda3: float = 1.0  # s_effective 权重——域常数，不随查询意图调整
    token_budget: int = 2000  # Stage 4 token 预算
    brief_prefix_chars: int = 80  # 压缩形态的内容前缀长度（截断占位）
    full_relevance_min: float = 0.5  # 保留完整细节的 relevance 下限（归一化后）
    full_s_min: float = 0.5  # 保留完整细节的 s_effective 下限
    max_supersedes_chain: int = 16  # supersedes 链解析防环上限


DEFAULT_RETRIEVE_CONFIG = RetrieveConfig()


@dataclass(frozen=True)
class NodeProps:
    """图侧取回的事件节点属性（φ + 内容）。

    consolidated=True（M6）：节点已固化——s 锁定（s_effective 取 s 现值，跳过衰减
    现算），两处 θ_effective 硬过滤对其跳过丢弃判定。"""

    node_key: str
    s: float
    n_last_touched: int
    content: str
    tau: datetime | None
    consolidated: bool = False


@dataclass(frozen=True)
class ScoredNode:
    """Stage 3 候选池节点。s_effective=None 表示实体叶子（不参与 FF）。"""

    node_key: str
    content: str
    tau: datetime | None
    s_effective: float | None
    sim: float
    path_score: float
    depth: int
    consolidated: bool = False  # 固化节点：θ 硬过滤跳过丢弃（M6）


@dataclass(frozen=True)
class EdgeRecord:
    """结果子图的边（out_key/in_key 是真实方向，supersedes 边如实返回）。"""

    out_key: str
    in_key: str
    label: str


@dataclass(frozen=True)
class NodeItem:
    """Stage 4 输出节点。brief=True 表示已压缩为简短提示。"""

    node_key: str
    content: str
    tau: datetime | None
    s_effective: float | None
    relevance: float
    brief: bool


@dataclass(frozen=True)
class RetrievalResult:
    """召回单元 = 带边子图（节点 + 时序/语义/因果/实体关系），非扁平节点列表。"""

    nodes: list[NodeItem]
    edges: list[EdgeRecord]


@dataclass(frozen=True)
class _NeighborRow:
    """束搜索一轮扩展取回的一行（src → 边 → dst）。"""

    src_key: str
    out_key: str
    in_key: str
    label: str
    dst: NodeProps
    superseded_by: tuple[str, ...]


# ---------------------------------------------------------------- 图查询脚本

# 批量取 φ + 内容（Stage 2 锚点用）；前置粗筛 n_star_cached > n_now
_FETCH_NODES_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_key', P.within(nodeKeys))
    .has('node_type', 'event')
    .has('n_star_cached', P.gt((nNow as long)))
    .project('node_key', 's', 'n', 'content', 'tau', 'consolidated')
    .by(values('node_key')).by(values('s')).by(values('n_last_touched'))
    .by(values('content')).by(values('tau'))
    .by(__.values('consolidated_at').fold())
    .toList()
['rows': rows]
"""

# 束搜索一轮扩展：frontier 顶点的三类关系边邻居（实体边不作扩展路径）
_EXPAND_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_key', P.within(frontierKeys))
    .as('src')
    .bothE('temporal', 'semantic', 'causal').as('e')
    .otherV()
    .has('space_id', spaceId)
    .has('node_type', 'event')
    .has('n_star_cached', P.gt((nNow as long)))
    .project('src_key', 'out_key', 'in_key', 'edge_label',
             'dst_key', 'dst_s', 'dst_n', 'dst_content', 'dst_tau', 'dst_consolidated',
             'superseded_by')
    .by(select('src').values('node_key'))
    .by(select('e').outV().values('node_key'))
    .by(select('e').inV().values('node_key'))
    .by(select('e').label())
    .by(values('node_key'))
    .by(values('s'))
    .by(values('n_last_touched'))
    .by(values('content'))
    .by(values('tau'))
    .by(__.values('consolidated_at').fold())
    .by(__.in('supersedes').values('node_key').fold())
    .toList()
['rows': rows]
"""

# 单节点属性 + supersedes 入边（重定向链解析用）
_RESOLVE_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_key', nk)
    .has('node_type', 'event')
    .project('node_key', 's', 'n', 'content', 'tau', 'consolidated', 'superseded_by')
    .by(values('node_key')).by(values('s')).by(values('n_last_touched'))
    .by(values('content')).by(values('tau'))
    .by(__.values('consolidated_at').fold())
    .by(__.in('supersedes').values('node_key').fold())
    .toList()
['rows': rows]
"""

# 实体叶子收集：候选事件节点的 entity 边 + 实体顶点（收敛后一次性挂回）
_ENTITY_LEAVES_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_key', P.within(nodeKeys))
    .outE('entity').as('e').inV()
    .project('out_key', 'entity_key')
    .by(select('e').outV().values('node_key'))
    .by(values('entity_key'))
    .toList()
['rows': rows]
"""


def _submit_rows(client: Client, script: str, bindings: dict) -> list[dict]:
    """提交脚本取回 rows（返回 map 按 entry 逐个流回，先合并）。"""
    result = client.submit(script, bindings).all().result()
    payload = {k: v for item in result for k, v in item.items()}
    return payload.get("rows", [])


# ---------------------------------------------------------------- Stage 2 锚点识别（ES）


def _s_eff(props: NodeProps, n_now: int, ff_config: ff.FFConfig) -> float:
    """现算 s_effective；固化节点 s 锁定、跳过衰减计算，取 s 现值（M6 定案）。"""
    if props.consolidated:
        return props.s
    return ff.s_effective(props.s, props.n_last_touched, n_now, config=ff_config)


def _rrf_merge(rankings: list[list[dict]], rrf_k: int) -> dict[str, float]:
    """RRF 融合：score(d) = Σ 1/(rrf_k + rank)。输入只有检索 rank——s 永不进 RRF。"""
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, hit in enumerate(ranking, start=1):
            key = hit["node_key"]
            scores[key] = scores.get(key, 0.0) + 1.0 / (rrf_k + rank)
    return scores


def _fetch_nodes(
    client: Client, gname: str, *, space_id: str, node_keys: list[str], n_now: int
) -> dict[str, NodeProps]:
    """批量取事件节点属性（带前置粗筛 n_star_cached > n_now）。"""
    rows = _submit_rows(
        client,
        _FETCH_NODES_SCRIPT,
        {"gname": gname, "spaceId": space_id, "nodeKeys": node_keys, "nNow": str(n_now)},
    )
    return {
        row["node_key"]: NodeProps(
            node_key=row["node_key"],
            s=row["s"],
            n_last_touched=row["n"],
            content=row["content"],
            tau=row["tau"],
            consolidated=bool(row["consolidated"]),
        )
        for row in rows
    }


def _stage2_anchors(
    client: Client,
    es: Elasticsearch,
    gname: str,
    *,
    space_id: str,
    query_text: str | None,
    query_vector: list[float] | None,
    n_now: int,
    theta: float,
    config: RetrieveConfig,
    ff_config: ff.FFConfig,
) -> list[ScoredNode]:
    """kNN + 关键词两路 → RRF 融合 → 取 φ → 现算 s_effective → 第一次 θ 硬过滤。"""
    rankings: list[list[dict]] = []
    if query_vector is not None:
        rankings.append(
            knn_search(es, space_id=space_id, query_vector=query_vector, k=config.knn_k)
        )
    if query_text:
        rankings.append(
            keyword_search(es, space_id=space_id, query_text=query_text, k=config.keyword_k)
        )
    rrf = _rrf_merge(rankings, config.rrf_k)
    top_keys = sorted(rrf, key=lambda k: (-rrf[k], k))[: config.anchor_top_k]
    if not top_keys:
        return []

    props_map = _fetch_nodes(client, gname, space_id=space_id, node_keys=top_keys, n_now=n_now)
    anchors: list[ScoredNode] = []
    for key in top_keys:
        props = props_map.get(key)
        if props is None:
            continue  # 图侧不存在或已跨遗忘视界（粗筛排除）
        s_eff = _s_eff(props, n_now, ff_config)
        if s_eff < theta and not props.consolidated:
            continue  # 第一次独立硬过滤（Stage 2 后置；固化节点跳过丢弃判定）
        anchors.append(
            ScoredNode(
                node_key=key,
                content=props.content,
                tau=props.tau,
                s_effective=s_eff,
                sim=rrf[key],  # 方案 A：锚点 sim = RRF 融合分
                path_score=rrf[key],
                depth=0,
                consolidated=props.consolidated,
            )
        )
    return anchors


# ---------------------------------------------------------------- Stage 3 自适应遍历（JanusGraph）


def transition_score(
    edge_label: str, sim: float, s_effective: float, config: RetrieveConfig
) -> float:
    """S(n_j|n_i,q) = exp(λ1·edge_prior + λ2·sim + λ3·log(s_eff))——软惩罚，不硬过滤。

    ρ 不在此处出现：θ_effective 只作用于 Stage 2 / Stage 3 收敛后两处硬过滤。
    """
    prior = config.edge_prior.get(edge_label, 0.0)
    log_s = math.log(max(s_effective, 1e-9))  # s 触 0 时兜底，避免 log(0)
    return math.exp(config.lambda1 * prior + config.lambda2 * sim + config.lambda3 * log_s)


def _resolve_chain(
    resolve_one: Callable[[str], tuple[NodeProps, tuple[str, ...]] | None],
    start_key: str,
    max_chain: int,
) -> list[NodeProps]:
    """沿 supersedes 入边链解析：返回 [起点, ..., 最新取代者]（防环上限 max_chain）。

    边方向是 n_new --supersedes--> n_old，被取代节点的 in('supersedes') 即取代者。
    """
    chain: list[NodeProps] = []
    seen: set[str] = set()
    key: str | None = start_key
    while key is not None and key not in seen and len(chain) < max_chain:
        seen.add(key)
        resolved = resolve_one(key)
        if resolved is None:
            break
        props, superseded_by = resolved
        chain.append(props)
        key = superseded_by[0] if superseded_by else None
    return chain


def _beam_search(
    *,
    anchors: list[ScoredNode],
    n_now: int,
    trace_history: bool,
    config: RetrieveConfig,
    ff_config: ff.FFConfig,
    expand_fn: Callable[[list[str]], list[_NeighborRow]],
    resolve_fn: Callable[[str], list[NodeProps]],
) -> tuple[dict[str, ScoredNode], set[EdgeRecord]]:
    """束搜索纯逻辑（图访问经 expand_fn/resolve_fn 注入，可单测）。

    扩展节点 sim=0（方案 A；方案 B 继承父 sim 衰减留待效果验证后探索，只改这里）。
    """
    pool: dict[str, ScoredNode] = {}
    edges: set[EdgeRecord] = set()

    def put(node: ScoredNode) -> bool:
        existing = pool.get(node.node_key)
        if existing is not None and existing.path_score >= node.path_score:
            return False
        pool[node.node_key] = node
        return True

    def enter(props: NodeProps, sim: float, path_score: float, depth: int) -> ScoredNode:
        s_eff = _s_eff(props, n_now, ff_config)
        node = ScoredNode(
            props.node_key,
            props.content,
            props.tau,
            s_eff,
            sim,
            path_score,
            depth,
            consolidated=props.consolidated,
        )
        put(node)
        return node

    # 锚点本身若已被取代，同样按 supersedes 规则重定向（Stage 3 内嵌处理）
    for anchor in anchors:
        chain = resolve_fn(anchor.node_key)
        if len(chain) > 1:
            edges.update(
                EdgeRecord(chain[i + 1].node_key, chain[i].node_key, "supersedes")
                for i in range(len(chain) - 1)
            )
            if trace_history:
                for props in chain[:-1]:
                    enter(props, anchor.sim, anchor.path_score, 0)
            enter(chain[-1], anchor.sim, anchor.path_score, 0)
        else:
            put(anchor)

    for depth in range(1, config.max_depth + 1):
        frontier = sorted(
            (n for n in pool.values() if n.depth == depth - 1),
            key=lambda n: (-n.path_score, n.node_key),
        )[: config.beam_width]
        if not frontier:
            break
        parent_by_key = {n.node_key: n for n in frontier}
        added = False
        seen_dst: set[str] = set()
        for row in expand_fn([n.node_key for n in frontier]):
            if row.dst.node_key in seen_dst:
                continue
            seen_dst.add(row.dst.node_key)
            parent = parent_by_key.get(row.src_key)
            if parent is None:
                continue
            edges.add(EdgeRecord(row.out_key, row.in_key, row.label))

            chain = [row.dst]
            if row.superseded_by:
                chain = resolve_fn(row.dst.node_key) or [row.dst]
                if len(chain) > 1:
                    edges.update(
                        EdgeRecord(chain[i + 1].node_key, chain[i].node_key, "supersedes")
                        for i in range(len(chain) - 1)
                    )
            targets = chain if trace_history else chain[-1:]
            for props in targets:
                s_eff = _s_eff(props, n_now, ff_config)
                score = parent.path_score * transition_score(row.label, 0.0, s_eff, config)
                node = ScoredNode(
                    props.node_key,
                    props.content,
                    props.tau,
                    s_eff,
                    0.0,
                    score,
                    depth,
                    consolidated=props.consolidated,
                )
                added = put(node) or added
        if not added:
            break
    return pool, edges


def _stage3_traverse(
    client: Client,
    gname: str,
    *,
    space_id: str,
    anchors: list[ScoredNode],
    n_now: int,
    trace_history: bool,
    config: RetrieveConfig,
    ff_config: ff.FFConfig,
) -> tuple[dict[str, ScoredNode], set[EdgeRecord]]:
    """Stage 3 自适应遍历。**签名无 es、无 rho**——两条实现约束由签名物理隔离：

    - 本阶段不访问 Elasticsearch（遍历完全由 JanusGraph 经 Cassandra 执行）；
    - ρ 只作用于两处硬过滤，不影响本阶段的软惩罚排序（θ 硬过滤在 retrieve()
      收敛后执行，不进本函数）。
    """

    def expand_fn(frontier_keys: list[str]) -> list[_NeighborRow]:
        rows = _submit_rows(
            client,
            _EXPAND_SCRIPT,
            {
                "gname": gname,
                "spaceId": space_id,
                "frontierKeys": frontier_keys,
                "nNow": str(n_now),
            },
        )
        return [
            _NeighborRow(
                src_key=row["src_key"],
                out_key=row["out_key"],
                in_key=row["in_key"],
                label=row["edge_label"],
                dst=NodeProps(
                    node_key=row["dst_key"],
                    s=row["dst_s"],
                    n_last_touched=row["dst_n"],
                    content=row["dst_content"],
                    tau=row["dst_tau"],
                    consolidated=bool(row["dst_consolidated"]),
                ),
                superseded_by=tuple(row["superseded_by"]),
            )
            for row in rows
        ]

    def resolve_one(key: str) -> tuple[NodeProps, tuple[str, ...]] | None:
        rows = _submit_rows(
            client, _RESOLVE_SCRIPT, {"gname": gname, "spaceId": space_id, "nk": key}
        )
        if not rows:
            return None
        row = rows[0]
        return (
            NodeProps(
                node_key=row["node_key"],
                s=row["s"],
                n_last_touched=row["n"],
                content=row["content"],
                tau=row["tau"],
                consolidated=bool(row["consolidated"]),
            ),
            tuple(row["superseded_by"]),
        )

    def resolve_fn(key: str) -> list[NodeProps]:
        return _resolve_chain(resolve_one, key, config.max_supersedes_chain)

    pool, edges = _beam_search(
        anchors=anchors,
        n_now=n_now,
        trace_history=trace_history,
        config=config,
        ff_config=ff_config,
        expand_fn=expand_fn,
        resolve_fn=resolve_fn,
    )

    # 实体叶子：收敛后把候选事件节点的 entity 边与实体顶点挂回结果子图
    if pool:
        for row in _submit_rows(
            client,
            _ENTITY_LEAVES_SCRIPT,
            {"gname": gname, "spaceId": space_id, "nodeKeys": list(pool)},
        ):
            entity_node_key = f"entity:{row['entity_key']}"
            edges.add(EdgeRecord(row["out_key"], entity_node_key, "entity"))
            if entity_node_key not in pool:
                pool[entity_node_key] = ScoredNode(
                    node_key=entity_node_key,
                    content=row["entity_key"],
                    tau=None,
                    s_effective=None,  # 实体顶点无 φ，不参与 FF
                    sim=0.0,
                    path_score=0.0,
                    depth=config.max_depth,
                )
    return pool, edges


# ---------------------------------------------------------------- Stage 4 token 预算


def _est_tokens(text: str) -> int:
    """token 估算占位（≈4 字符/token）；不引入 tokenizer 依赖，正式口径待标定。"""
    return max(1, len(text) // 4)


def _stage4_budget(candidates: list[ScoredNode], *, config: RetrieveConfig) -> list[NodeItem]:
    """双权重贪心装预算：relevance 与 s_effective 都高 → 完整；相关但 s 低 → 压缩。"""

    def relevance(node: ScoredNode) -> float:
        if node.s_effective is None:
            return 0.0  # 实体叶子不参与排序权重，随预算挂尾
        return node.path_score / max_score if max_score > 0 else 0.0

    max_score = max((c.path_score for c in candidates if c.s_effective is not None), default=0.0)
    ordered = sorted(
        candidates,
        key=lambda n: (-relevance(n) * (n.s_effective or 0.0), n.node_key),
    )
    items: list[NodeItem] = []
    used = 0
    for node in ordered:
        rel = relevance(node)
        full = node.s_effective is None or (
            rel >= config.full_relevance_min and node.s_effective >= config.full_s_min
        )
        content = node.content if full else node.content[: config.brief_prefix_chars]
        cost = _est_tokens(content)
        if used + cost > config.token_budget and full and node.s_effective is not None:
            # 完整形态放不下 → 降级为压缩形态再试
            full = False
            content = node.content[: config.brief_prefix_chars]
            cost = _est_tokens(content)
        if used + cost > config.token_budget:
            continue
        used += cost
        items.append(
            NodeItem(node.node_key, content, node.tau, node.s_effective, rel, brief=not full)
        )
    return items


# ---------------------------------------------------------------- 主入口


def retrieve(
    client: Client,
    es: Elasticsearch,
    gname: str,
    *,
    space_id: str,
    query_text: str | None = None,
    query_vector: list[float] | None = None,
    n_now: int,
    rho: float = 1.0,
    theta_base: float = ff.DEFAULT_CONFIG.theta_base,
    trace_history: bool = False,
    config: RetrieveConfig = DEFAULT_RETRIEVE_CONFIG,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> RetrievalResult:
    """M4 四阶段检索主入口（M5 memory.retrieve 的内部实现）。

    Stage 1（隐式）：space_id 必填非空（硬边界）；query_text/query_vector 至少其一。
    """
    if not space_id:
        raise ValueError("space_id 必填：检索范围硬边界，禁止跨 space 全局检索")
    if query_text is None and query_vector is None:
        raise ValueError("query_text / query_vector 至少提供其一")

    theta = ff.theta_effective(theta_base, rho)  # ρ 的唯一入口：两处硬过滤阈值

    # Stage 2：锚点识别（ES）+ 第一次独立硬过滤
    anchors = _stage2_anchors(
        client,
        es,
        gname,
        space_id=space_id,
        query_text=query_text,
        query_vector=query_vector,
        n_now=n_now,
        theta=theta,
        config=config,
        ff_config=ff_config,
    )
    if not anchors:
        return RetrievalResult(nodes=[], edges=[])

    # Stage 3：自适应遍历（JanusGraph；签名无 es、无 rho）
    pool, edges = _stage3_traverse(
        client,
        gname,
        space_id=space_id,
        anchors=anchors,
        n_now=n_now,
        trace_history=trace_history,
        config=config,
        ff_config=ff_config,
    )

    # Stage 3 收敛后：第二次独立硬过滤（与 Stage 2 不合并；固化节点跳过丢弃判定）
    final = {
        key: node
        for key, node in pool.items()
        if node.s_effective is None or node.consolidated or node.s_effective >= theta
    }
    final_edges = sorted(
        (e for e in edges if e.out_key in final and e.in_key in final),
        key=lambda e: (e.out_key, e.in_key, e.label),
    )

    # Stage 4：token 预算
    return RetrievalResult(
        nodes=_stage4_budget(list(final.values()), config=config), edges=final_edges
    )
