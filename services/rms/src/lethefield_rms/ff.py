"""FF 计算引擎（开发文档 §4，M3）：遗忘函数现算 + δ 动态更新。

核心纪律：
- **衰减不物化**：`s_effective` 一律读取时现算，任何后台任务不得把衰减结果写回
  节点的 `s` 字段；`s` 只能被 δ 三值（+0.2 强化 / −0.5 冲突失效 / −0.1 忽视惩罚）修改。
- **参数分层不混层**（开发文档 §4 参数分层表）：
  - Memory-object runtime：`s`、`n_last_touched` —— 节点属性，随 δ 调整变化；
  - Agent-level constants：`λ`、`N_neglect`（及公式占位 t/t₀）—— `FFConfig`，构建期固定；
  - Query-time controls：`θ_base`、`ρ`（`θ_effective = θ_base / ρ`）—— 每次检索请求传入，
    不进 `FFConfig` 的衰减语义（`theta_base` 字段仅作 `n_star_cached` 重算的默认 θ）。
- **三条 δ 触发路径是独立函数**，不合并成通用"打分接口"：
  `apply_reinforce`（同步直连 RMS，M5 入口）/ `apply_conflict`（consolidation 异步，M7）/
  `apply_neglect`（FS sweep 异步，M6，**不更新 n_last_touched**，否则惩罚自我抵消）。
- **n_star_cached 仅限前置粗筛**（`WHERE n_star_cached > $n_now`），不参与实时计算；
  任何 δ 调整立即重算（设计文档 §13.6）。缓存值是**绝对遗忘视界**
  （n_last_touched + n*），否则与绝对事件序号 n_now 的比较不成立；
  ceil 取整——粗筛宁可多留候选，不可误杀尚未跨界的节点。
- **固化锁定**（M6）：节点带 `consolidated_at` 时，δ 不改 s、不动
  n_last_touched / n_star_cached，仅计数器照计（计数是事实记录，不是状态修改）。

占位参数（λ=0.16、θ_base=0.3、t/t₀=1.0、N_neglect=20）沿用 spike/M0 测试口径，
正式标定属种子期；s 合法区间 [0,1] 按显著性语义设定（设计未明文给定，可配）。
"""

import math
from dataclasses import dataclass
from datetime import datetime

from gremlin_python.driver.client import Client
from lethefield_metrics import counter as _metric_counter
from prometheus_client import REGISTRY as _DEFAULT_REGISTRY

# Java Long 上限：λ=0（不衰减）时绝对遗忘视界为 +∞，以此落库
_LONG_MAX = 2**63 - 1

# δ 三值（设计文档 §2.3，直接实现，不做变体）
DELTA_REINFORCE = +0.2  # 强化：memory.reinforce 调用（M5），同步
DELTA_CONFLICT = -0.5  # 冲突失效：纠错事件被 consolidation 处理（M7），异步
DELTA_NEGLECT = -0.1  # 忽视惩罚：FS sweep 周期触发（M6），异步

# s 截断指标（设计文档 §19：ff_s_clamp_total{bound}，bound ∈ upper/lower）。
# 注册进 prometheus 默认 registry（libs/metrics 的 registry=None 不注册），
# 服务暴露口由 M12 统一接线。
_S_CLAMP_TOTAL = _metric_counter(
    "lethefield_ff_s_clamp_total",
    "s 触上下限截断次数（FF δ 调整时计数）",
    labels=["bound"],
    registry=_DEFAULT_REGISTRY,
)


@dataclass(frozen=True)
class FFConfig:
    """Agent-level constants：构建期按 agent 域固定，查询时不改变。

    数值全为占位（spike/M0 测试口径），正式标定属种子期，调整走参数标定流程。
    """

    lambda_decay: float = 0.16  # λ 衰减率
    n_neglect: int = 20  # N_neglect 忽视间隔（M6 sweep 触发条件用，引擎本身不消费）
    grace_n: int = 40  # 归档宽限期（事件距离，M6 定案；占位 2×N_neglect，§20 待标定）
    t_over_t0: float = 1.0  # 公式时间因子 t/t₀ 占位
    theta_base: float = 0.3  # n_star_cached 重算的默认 θ（查询时 θ 另由 ρ 推导）
    s_min: float = 0.0  # s 合法区间下界
    s_max: float = 1.0  # s 合法区间上界


DEFAULT_CONFIG = FFConfig()


@dataclass(frozen=True)
class PhiState:
    """节点 φ_i 状态块（设计文档 §4.1）的读取快照。

    consolidated_at 非空 = 已固化（M6）：s 锁定、跳过衰减与 sweep，
    n_star_cached 固化时置 LONG_MAX。"""

    s: float
    n_last_touched: int
    n_star_cached: int
    reinforce_count: int
    conflict_count: int
    neglect_count: int
    consolidated_at: datetime | None = None  # 缺省兼容旧构造点；非空 = 已固化


# ---------------------------------------------------------------- 纯函数（现算，不触存储）


def s_effective(
    s: float,
    n_last_touched: int,
    n_now: int,
    *,
    config: FFConfig = DEFAULT_CONFIG,
) -> float:
    """s_effective = s × e^(−λ·Δn·log(1+t/t₀))，Δn = n_now − n_last_touched。

    读取时现算——这是 `s_effective` 的唯一获取方式，存储中不存在该字段。
    """
    delta_n = n_now - n_last_touched
    return s * math.exp(-config.lambda_decay * delta_n * math.log1p(config.t_over_t0))


def theta_effective(theta_base: float, rho: float) -> float:
    """θ_effective = θ_base / ρ（查询时参数，每次检索请求传入）。"""
    return theta_base / rho


def n_star(s: float, theta: float, *, config: FFConfig = DEFAULT_CONFIG) -> float:
    """相对遗忘视界 n* = ln(s/θ) / (λ·ln(1+t/t₀))：s 跌破 θ 前还能存活的事件距离数。

    s ≤ θ（已在阈值之下）返回 0.0；λ=0 或时间因子为 0（不衰减）返回 +inf。
    """
    if s <= theta:
        return 0.0
    denom = config.lambda_decay * math.log1p(config.t_over_t0)
    if denom == 0:
        return math.inf
    return math.log(s / theta) / denom


def n_star_horizon(
    s: float,
    n_last_touched: int,
    theta: float,
    *,
    config: FFConfig = DEFAULT_CONFIG,
) -> int:
    """绝对遗忘视界（写入 n_star_cached 的值）= n_last_touched + ceil(n*)。

    供前置粗筛 `WHERE n_star_cached > $n_now` 使用；ceil 保证不误杀未跨界节点。
    """
    horizon = n_star(s, theta, config=config)
    if math.isinf(horizon):
        return _LONG_MAX
    return n_last_touched + math.ceil(horizon)


def clamp_s(s: float, *, config: FFConfig = DEFAULT_CONFIG) -> tuple[float, str | None]:
    """把 δ 调整后的 s 截断到 [s_min, s_max]；截断发生时计 ff_s_clamp_total{bound}。

    返回 (截断后的 s, 触界方向 "upper"/"lower"/None)。
    """
    if s > config.s_max:
        _S_CLAMP_TOTAL.labels(bound="upper").inc()
        return config.s_max, "upper"
    if s < config.s_min:
        _S_CLAMP_TOTAL.labels(bound="lower").inc()
        return config.s_min, "lower"
    return s, None


def archive_eligible(n_now: int, n_star_cached: int, grace_n: int) -> bool:
    """归档资格纯判定（设计文档 §13.4，M6 定案：宽限期为事件距离 grace_n）。

    `n_now ≥ n_star_cached + grace_n` 才归档。两个免费正确性：
    - 固化节点 n_star_cached = LONG_MAX → 永不满足，天然排除；
    - 宽限期内任何 reinforce/conflict 会把 n_star_cached 推过 n_now，资格自动失效
      （零额外状态、零竞态窗口）。

    **M7 重放重建脚本必须复用本函数**，归档判定从 EX 事件流确定性重推，禁止抄一份。
    """
    return n_now >= n_star_cached + grace_n


# ---------------------------------------------------------------- 图读写（δ 三条触发路径）

_READ_PHI_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
t.V().has('space_id', spaceId).has('node_key', nodeKey)
    .valueMap('s', 'n_last_touched', 'n_star_cached',
              'reinforce_count', 'conflict_count', 'neglect_count', 'consolidated_at')
    .next()
"""

# δ 落库脚本：只持久化 Python 侧已算好的结果（公式与截断全部在引擎内，Groovy 不算 FF）。
# long 型以字符串绑定传输 + Groovy `as long` 强转（gremlin_python int32 序列化限制）。
# locked=true（节点已固化）：s / n_last_touched / n_star_cached 一律不写，仅计数器 +1
# （M6 定案：固化后 ±δ 不改 s，计数是事实记录照计）。
_APPLY_DELTA_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def v = t.V().has('space_id', spaceId).has('node_key', nodeKey).next()
if (!locked) {
    v.property('s', sNew as double)
    v.property('n_star_cached', nStar as long)
    if (touchFlag) { v.property('n_last_touched', nNow as long) }
}
v.property(counterKey, (v.value(counterKey) as int) + 1)
t.tx().commit()
'ok'
"""


def read_phi(client: Client, gname: str, *, space_id: str, node_key: str) -> PhiState:
    """读取节点 φ_i 状态块（valueMap 结果按 entry 逐个流回，先合并再取单元素值）。"""
    result = (
        client.submit(_READ_PHI_SCRIPT, {"gname": gname, "spaceId": space_id, "nodeKey": node_key})
        .all()
        .result()
    )
    if not result:
        raise KeyError(f"节点不存在：space={space_id} node_key={node_key}")
    merged = {k: v for item in result for k, v in item.items()}
    return PhiState(
        s=merged["s"][0],
        n_last_touched=merged["n_last_touched"][0],
        n_star_cached=merged["n_star_cached"][0],
        reinforce_count=merged["reinforce_count"][0],
        conflict_count=merged["conflict_count"][0],
        neglect_count=merged["neglect_count"][0],
        consolidated_at=merged["consolidated_at"][0] if "consolidated_at" in merged else None,
    )


def compute_delta(
    phi: PhiState,
    *,
    delta: float,
    touch: bool,
    counter_key: str,
    n_now: int,
    config: FFConfig = DEFAULT_CONFIG,
) -> PhiState:
    """δ 调整纯决策（不触存储）：固化节点 s/视界锁定、仅计数器 +1；其余照常算。"""
    if phi.consolidated_at is not None:
        new_s, n_touched, n_star = phi.s, phi.n_last_touched, phi.n_star_cached
    else:
        new_s, _bound = clamp_s(phi.s + delta, config=config)
        n_touched = n_now if touch else phi.n_last_touched
        n_star = n_star_horizon(new_s, n_touched, config.theta_base, config=config)
    return PhiState(
        s=new_s,
        n_last_touched=n_touched,
        n_star_cached=n_star,
        reinforce_count=phi.reinforce_count + (counter_key == "reinforce_count"),
        conflict_count=phi.conflict_count + (counter_key == "conflict_count"),
        neglect_count=phi.neglect_count + (counter_key == "neglect_count"),
        consolidated_at=phi.consolidated_at,
    )


def _apply_delta(
    client: Client,
    gname: str,
    *,
    space_id: str,
    node_key: str,
    delta: float,
    touch: bool,
    counter_key: str,
    n_now: int,
    config: FFConfig = DEFAULT_CONFIG,
) -> PhiState:
    """读 φ → 引擎内算 δ/截断/n_star → 落库，返回更新后的状态。"""
    phi = read_phi(client, gname, space_id=space_id, node_key=node_key)
    new = compute_delta(
        phi, delta=delta, touch=touch, counter_key=counter_key, n_now=n_now, config=config
    )
    result = (
        client.submit(
            _APPLY_DELTA_SCRIPT,
            {
                "gname": gname,
                "spaceId": space_id,
                "nodeKey": node_key,
                "sNew": new.s,
                "nStar": str(new.n_star_cached),
                "touchFlag": touch,
                "nNow": str(n_now),
                "counterKey": counter_key,
                "locked": phi.consolidated_at is not None,
            },
        )
        .all()
        .result()
    )
    if "ok" not in result:
        raise RuntimeError(f"δ 更新未返回 ok：{result}")
    return new


def apply_reinforce(
    client: Client,
    gname: str,
    *,
    space_id: str,
    node_key: str,
    n_now: int,
    config: FFConfig = DEFAULT_CONFIG,
) -> PhiState:
    """δ = +0.2 强化（memory.reinforce，M5）：更新 n_last_touched，同步直连 RMS。

    异步追加轻量元事件到 EX（fire-and-forget、时间窗合并）由调用方负责（§13.7），
    不在本函数内。
    """
    return _apply_delta(
        client,
        gname,
        space_id=space_id,
        node_key=node_key,
        delta=DELTA_REINFORCE,
        touch=True,
        counter_key="reinforce_count",
        n_now=n_now,
        config=config,
    )


def apply_conflict(
    client: Client,
    gname: str,
    *,
    space_id: str,
    node_key: str,
    n_now: int,
    config: FFConfig = DEFAULT_CONFIG,
) -> PhiState:
    """δ = −0.5 冲突失效（M7 consolidation 异步施加）：更新 n_last_touched。"""
    return _apply_delta(
        client,
        gname,
        space_id=space_id,
        node_key=node_key,
        delta=DELTA_CONFLICT,
        touch=True,
        counter_key="conflict_count",
        n_now=n_now,
        config=config,
    )


def apply_neglect(
    client: Client,
    gname: str,
    *,
    space_id: str,
    node_key: str,
    n_now: int,
    config: FFConfig = DEFAULT_CONFIG,
) -> PhiState:
    """δ = −0.1 忽视惩罚（M6 FS sweep 异步施加）：**不更新 n_last_touched**（否则惩罚自我抵消）。"""
    return _apply_delta(
        client,
        gname,
        space_id=space_id,
        node_key=node_key,
        delta=DELTA_NEGLECT,
        touch=False,
        counter_key="neglect_count",
        n_now=n_now,
        config=config,
    )
