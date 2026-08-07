"""④ 入料口：EX 只读派生纠错对（用户记忆内容副本，仅授权 space）。

这是训练管线与 EX 的唯一接触面：**只读 EX、产出独立副本、永不回写**（§12.4）。
1.0 范围仅纠错前后对（供 R3 关联）；纠错链/高质量片段独立样本随 R4 延后。

旧内容纯 EX 推导：ref_conflict = 旧 node_key = "ev_{旧 event_id}"（node_key_of
过渡约定），旧 event 行仍在 EX 经验事件表内——不触 RMS 图。

授权拦截在**入 topic 前**：CONTENT_COPY 未授权直接拒发（既定拦截点）。
红线 1：入口绑定显式 space，不存在扫全部空间的形态。
"""

import json
from collections.abc import Callable
from pathlib import Path

from lethefield_clients import (
    AuthRegistryStore,
    AuthScope,
    ExEvent,
    FeedEvent,
    FeedKind,
    FeedSource,
    list_experience_events,
    space_ref_of,
)


def collect_correction_pairs(events: list[ExEvent]) -> list[dict]:
    """经验事件 → 纠错前后对（纯函数，可单测）。

    ref_conflict 指向的旧事件不在 EX（已归档/键不符过渡约定）时跳过——
    对子内容必须完整，缺一半的对不喂。
    """
    by_event_id = {e.event_id: e for e in events}
    pairs = []
    for e in events:
        if not e.ref_conflict:
            continue
        old = by_event_id.get(e.ref_conflict.removeprefix("ev_"))
        if old is None:
            continue
        pairs.append(
            {
                "old_node_key": e.ref_conflict,
                "new_node_key": f"ev_{e.event_id}",
                "before": old.content,
                "after": e.content,
                "corrected_at": e.created_at.isoformat(),
                "n": e.n,
            }
        )
    return pairs


def _pair_key(pair: dict) -> str:
    return f"{pair['old_node_key']}:{pair['new_node_key']}"


def _load_state(state_path: Path) -> set[str]:
    if not state_path.exists():
        return set()
    return set(json.loads(state_path.read_text(encoding="utf-8")))


def run(
    session,
    *,
    space_id: str,
    registry: AuthRegistryStore,
    publish: Callable[[FeedEvent], None],
    state_path: str | Path,
) -> int:
    """单 space 纠错对喂入（幂等：已喂对子记录于 state 文件，重跑不重复）。

    返回新喂入条数；未授权抛 PermissionError（CLI 转退出码 1）。
    """
    space_ref = space_ref_of(space_id)
    if not registry.is_authorized(space_ref, AuthScope.CONTENT_COPY):
        raise PermissionError(f"space 未授权 content_copy（④ 类入 topic 前拦截）：{space_ref}")
    events = list_experience_events(session, space_id=space_id)
    state_path = Path(state_path)
    sent = _load_state(state_path)
    count = 0
    for pair in collect_correction_pairs(events):
        if _pair_key(pair) in sent:
            continue
        publish(
            FeedEvent(
                kind=FeedKind.CORRECTION_PAIR,
                source=FeedSource.EX_DERIVED,
                space_ref=space_ref,
                payload=pair,
            )
        )
        sent.add(_pair_key(pair))
        count += 1
    if count:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(sorted(sent)), encoding="utf-8")
    return count
