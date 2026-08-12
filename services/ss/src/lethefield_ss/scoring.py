"""打分编排纯逻辑（M14）：parse → 降级分级 → 权重合成 → ScoringResult 信封。

降级分级（v1.2 修订记录第 22 条定案）：
- 六维齐全 → 正常；
- 缺 1 维且 policy=neutral_mark → 该维置中性值 + degraded 标记 + 缺失维清单；
- 缺 ≥2 维，或 policy=retry 下有缺维 → 抛 ScoringError（走重试 → DLQ）；
- 不可解析由 prompt.parse_scores 抛 ValueError，调用方归入同一失败路径。

权重合成 s = Σ wᵢ·xᵢ 后 clamp [0,1]；权重来自配置（禁硬编码定案），原始六维
与合成 s 在 ScoringResult/details 中分开存储。
"""

import time

from lethefield_clients.ex_stream import ScoringResult
from lethefield_rms.rebuild import node_key_of

from lethefield_ss.config import DEGRADE_NEUTRAL_MARK, SSConfig
from lethefield_ss.llm import ScoringError
from lethefield_ss.prompt import parse_scores


def compose_s(dims: dict[str, float], weights: dict[str, float]) -> float:
    """六维 → 合成 s 初值（权重配置注入；clamp [0,1] 防权重未归一时的溢出）。"""
    s = sum(weights[d] * dims[d] for d in dims)
    return min(1.0, max(0.0, s))


def build_result(
    event,
    *,
    dims: dict[str, float],
    missing: list[str],
    model_version: str,
    config: SSConfig,
) -> ScoringResult:
    """缺失分级 + 合成 + 组装信封（纯函数；scorer 已成功返回的后续处理）。"""
    degraded = False
    if missing:
        if len(missing) >= 2 or config.degrade_policy != DEGRADE_NEUTRAL_MARK:
            raise ScoringError(f"打分缺 {len(missing)} 维（{missing}），按失败处理")
        dims = {**dims, missing[0]: config.degrade_neutral}
        degraded = True
    return ScoringResult(
        space_id=event.space_id,
        event_id=event.event_id,
        n=event.n,
        node_key=node_key_of(event.event_id),
        dims=dims,
        s=compose_s(dims, config.weights),
        model_version=model_version,
        degraded=degraded,
        missing_dims=list(missing),
        scored_at_ms=int(time.time() * 1000),
    )


def score_event(event, *, scorer, config: SSConfig) -> tuple[ScoringResult, dict[str, int]]:
    """单事件打分全流程：LLM 调用 → 解析 → 降级分级 → 合成。返回 (信封, token usage)。

    scorer 协议：`score(content) -> (raw_text, usage, model)`（LLMScorer / fake 同款）。
    """
    raw, usage, model = scorer.score(event.content)
    try:
        dims, missing = parse_scores(raw)
    except ValueError as e:
        raise ScoringError(f"LLM 响应不可解析：{e}") from e
    result = build_result(event, dims=dims, missing=missing, model_version=model, config=config)
    return result, usage
