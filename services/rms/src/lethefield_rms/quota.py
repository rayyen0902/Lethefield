"""红线 2 单 space 资源配额（M13 定案，开发文档 §14）。

设计依据（M13 升级确认定案）：
- 配额语义是**先查后写**：写入前查当前计数，已达上限即拒绝本次写入（QuotaExceeded）。
- **图 count 短 TTL 缓存 = 近似执行、超发有界，禁止当精确语义依赖**——TTL 窗口内
  并发写入可短暂越过上限（超发量 ≤ 窗口内写入速率 × TTL），这是有意的开销/精度取舍；
  向量条数走 ES `_count?routing=`（O(1)，不缓存，见 metrics_exporter 先例）。
- 配置层级 = 全局默认 + 按 tier 可选覆盖（TIER_QUOTA_OVERRIDES），1.0 不做 per-space。
- API 侧 QuotaExceeded → 429 rate_limited（message 含 quota_exceeded 字样），
  处理器在 services/api http_app.py。
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients.control_plane import Tier


@dataclass(frozen=True)
class QuotaConfig:
    """单 space 资源配额上限（红线 2）。

    数值全为占位待标定，调整走参数标定流程。count_cache_ttl_seconds 是图计数
    进程内缓存的 TTL：缓存意味着配额是**近似执行**（超发有界），不是精确门禁，
    任何依赖精确计数的语义不得建立在配额检查之上。
    """

    max_vertices: int
    max_edges: int
    max_vectors: int
    count_cache_ttl_seconds: float = 30.0


# 占位待标定：默认设高，既有集成测试直调 writer 不受影响（M13 风险节定案）
DEFAULT_QUOTA_CONFIG = QuotaConfig(
    max_vertices=1_000_000,
    max_edges=5_000_000,
    max_vectors=1_000_000,
)

# 按 tier 可选覆盖（机制保留，1.0 无差异化档位，缺省回落全局默认）
TIER_QUOTA_OVERRIDES: dict[Tier, QuotaConfig] = {}


def quota_for_tier(tier: Tier | None) -> QuotaConfig:
    """解析某 tier 的生效配额：覆盖表命中返回覆盖值，否则回落全局默认。"""
    if tier is None:
        return DEFAULT_QUOTA_CONFIG
    return TIER_QUOTA_OVERRIDES.get(tier, DEFAULT_QUOTA_CONFIG)


class QuotaExceeded(Exception):
    """配额拒绝（红线 2）。str 必含 quota_exceeded 字样（API 429 message 契约）。"""

    def __init__(self, kind: str, space_id: str, limit: int, actual: int) -> None:
        super().__init__(
            f"quota_exceeded: space {space_id!r} 的 {kind} 已达上限 {limit}（当前 {actual}）"
        )
        self.kind = kind
        self.space_id = space_id
        self.limit = limit
        self.actual = actual


_LIMITS = {"vertex": "max_vertices", "edge": "max_edges", "vector": "max_vectors"}

# 图计数脚本形态同 scheduler/migrate.py 的 _graph_counts：per-space 图
# ConfiguredGraphFactory.open(gname).traversal().V()/.E().count()（图名即 space，
# 红线 1 扫描器对纯 .V().count()/.E().count() 豁免）。
_COUNT_SCRIPTS = {
    "vertex": "def g = ConfiguredGraphFactory.open(gname); g.traversal().V().count().next()",
    "edge": "def g = ConfiguredGraphFactory.open(gname); g.traversal().E().count().next()",
}

# 进程级共享计数缓存（默认）：writer/vectors 的默认路径每次调用现场构造
# QuotaCounters，实例级缓存永远不命中——每写一次全图 count 一次（rebuild 退化
# O(n²)，M13 集成套件实测被拖慢）。TTL 缓存只有进程级共享才有意义；dict
# get/set 有 GIL 兜底，并发最坏后果是重复查一次，近似执行语义本就允许。
# 单测注入独立 cache={} 隔离。
_SHARED_COUNT_CACHE: dict[tuple[str, str], tuple[float, int]] = {}


def check_quota(kind: str, current: int, config: QuotaConfig, *, space_id: str) -> None:
    """先查后写校验（纯函数）：current >= 对应上限时抛 QuotaExceeded。

    kind ∈ vertex/edge/vector；未知 kind 抛 ValueError（fail-closed，不静默放行）。
    """
    try:
        limit = getattr(config, _LIMITS[kind])
    except KeyError:
        raise ValueError(f"未知配额 kind {kind!r}，必须在 {sorted(_LIMITS)} 内") from None
    if current >= limit:
        raise QuotaExceeded(kind, space_id, limit, current)


class QuotaCounters:
    """配额计数提供者：图顶点/边数 + 向量条数。

    - vertex_count/edge_count：per-space 图 count 遍历 + 进程级共享 TTL 缓存
      （key=(gname, kind)，TTL=config.count_cache_ttl_seconds；默认共享
      _SHARED_COUNT_CACHE——writer/vectors 默认路径每调用新建 counters，实例级
      缓存永不命中）——近似执行语义见 QuotaConfig docstring。
    - vector_count：`es.count(index=rms_vectors, query=term space_id, routing=space_id)`，
      O(1) 不缓存（routing 收拢 + term 过滤双机制，见 vector_count docstring）。
    - client/es 允许为 None：只有对应计数路径需要（writer 只查图计数可不给 es，
      vectors 只查向量计数可不给 client）；缺失却调用对应路径时抛 ValueError。

    **不要在线程间共享同一个 QuotaCounters 持有的 client**——gremlin_python
    单连接跨线程死锁是已知坑（M10 实测）：并发线程各用各的连接与各的 counters。
    """

    def __init__(
        self,
        client: Client | None,
        es: Elasticsearch | None,
        config: QuotaConfig = DEFAULT_QUOTA_CONFIG,
        *,
        clock: Callable[[], float] = time.monotonic,
        cache: dict[tuple[str, str], tuple[float, int]] | None = None,
    ) -> None:
        self._client = client
        self._es = es
        self._config = config
        self._clock = clock  # 时间源注入（TTL 测试用）
        # 默认进程级共享缓存（见 _SHARED_COUNT_CACHE 注释）；单测注入独立 dict 隔离
        self._cache = cache if cache is not None else _SHARED_COUNT_CACHE

    def _graph_count(self, gname: str, kind: str) -> int:
        now = self._clock()
        cached = self._cache.get((gname, kind))
        if cached is not None and now - cached[0] < self._config.count_cache_ttl_seconds:
            return cached[1]
        if self._client is None:
            raise ValueError(f"{kind} 计数需要 gremlin client，本 QuotaCounters 未注入")
        count = int(self._client.submit(_COUNT_SCRIPTS[kind], {"gname": gname}).all().result()[0])
        self._cache[(gname, kind)] = (now, count)
        return count

    def vertex_count(self, gname: str) -> int:
        return self._graph_count(gname, "vertex")

    def edge_count(self, gname: str) -> int:
        return self._graph_count(gname, "edge")

    def vector_count(self, space_id: str) -> int:
        """rms_vectors 内该 space 的文档数（routing 收拢 + space_id term 过滤，O(1) 不缓存）。

        routing 只把查询收拢到目标分片，分片是多 space 共享的——不带 space_id
        term 过滤会把同分片其他 space 的文档计入（M13 集成测试实测：共享分片上
        27 条他 space 文档导致本 space 配额误拒）。与 knn_search 同款双机制。
        """
        if self._es is None:
            raise ValueError("vector 计数需要 es client，本 QuotaCounters 未注入")
        # 惰性 import：vectors 与本模块互相引用（vectors.index_vector 注入配额），
        # 模块级 import 会成环
        from lethefield_rms.vectors import VECTORS_INDEX

        return int(
            self._es.count(
                index=VECTORS_INDEX,
                query={"term": {"space_id": space_id}},
                routing=space_id,
            )["count"]
        )
