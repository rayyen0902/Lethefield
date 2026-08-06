"""纠错处理器（M7，开发文档 §8 / 设计文档 §13.7）：consolidation 纠错职责的最小独立形态。

- 纠错 = 携带 `ref_conflict` 引用的普通 EX 经验事件（M5 flag_conflict 已落 EX）；
  本模块扫 EX 纠错事件 → 建 `n_new --supersedes--> n_old` 边 + 对旧节点异步施加
  −0.5、`conflict_count += 1`。
- **同一对节点幂等**：原子脚本在单 tx 内检查 supersedes 边已存在 → `duplicate`
  零写入（不重复建边、不重复扣分）；边即幂等标记，无额外状态表。
- 节点解析：新节点按 `ref_ex = event_id`（RMS↔EX 唯一关联机制），旧节点按
  `node_key = ref_conflict`；任一缺失 → pending（M15 入链前新节点可能尚未落图，
  下轮再处理，EX 不可变记录保证不丢）。
- FF 公式单点不变：−0.5 由 `ff.compute_delta` 在 Python 侧预算（含固化锁定语义），
  Groovy 只落库不算公式。
- 不设硬失效标志：supersedes 边记录事实，"是否返回、如何降权"下沉检索策略
  （M4 Stage 3 已实现重定向与 trace_history）。

常驻循环/心跳留待 M15 写入链合并时统一落地；本模块提供单轮处理 + CLI。
"""

import argparse
from dataclasses import dataclass

from cassandra.cluster import Session
from gremlin_python.driver.client import Client
from lethefield_clients import (
    ExKeyspaceControlPlaneStore,
    StaticControlPlaneStore,
    ex_cassandra_cluster,
    gremlin_client,
    list_experience_events,
    n_now,
    redis_client,
)

from lethefield_rms import ff

# 新节点按 ref_ex 反查（space 内扫描：ref_ex 无索引，纠错低频可接受；
# 红线兼容——has('space_id') 开头，非跨 space 全局扫描）
_FIND_BY_REFEX_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def rows = t.V().has('space_id', spaceId).has('node_type', 'event')
    .has('ref_ex', refEx).values('node_key').toList()
['rows': rows]
"""

# 原子幂等施加：单 tx 内"查边 → 建边 → 落 δ"——边已存在则 duplicate 零写入。
# δ 数值全部在 Python 侧由 ff.compute_delta 预算（含固化锁定：locked=true 只计计数器），
# 本脚本不内嵌任何 FF 公式。long 型字符串绑定 + `as long` 强转（int32 序列化限制）。
_APPLY_CORRECTION_SCRIPT = """
def t = ConfiguredGraphFactory.open(gname).traversal()
def newV = t.V().has('space_id', spaceId).has('node_key', newKey).next()
def oldV = t.V().has('space_id', spaceId).has('node_key', oldKey).next()
def existing = t.V(newV).outE('supersedes').where(__.inV().has('node_key', oldKey)).toList()
if (!existing.isEmpty()) { return 'duplicate' }
newV.addEdge('supersedes', oldV)
if (!locked) {
    oldV.property('s', sNew as double)
    oldV.property('n_star_cached', nStar as long)
    oldV.property('n_last_touched', nNow as long)
}
oldV.property('conflict_count', (oldV.value('conflict_count') as int) + 1)
t.tx().commit()
'ok'
"""


@dataclass
class CorrectionsStats:
    """单 space 一轮纠错处理计数。"""

    applied: int = 0  # 新建 supersedes 边并施加 −0.5
    duplicate: int = 0  # 同对节点重复纠错（幂等跳过）
    pending: int = 0  # 新/旧节点未落图（下轮再处理）


def _find_node_by_refex(client: Client, gname: str, *, space_id: str, ref_ex: str) -> str | None:
    """按 ref_ex（EX event_id）反查图节点 node_key；不存在返回 None。"""
    result = (
        client.submit(_FIND_BY_REFEX_SCRIPT, {"gname": gname, "spaceId": space_id, "refEx": ref_ex})
        .all()
        .result()
    )
    rows = [v for item in result for v in item.get("rows", [])]
    return rows[0] if rows else None


def apply_correction(
    client: Client,
    gname: str,
    *,
    space_id: str,
    new_node_key: str,
    old_node_key: str,
    n_now: int,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> str:
    """施加单条纠错：建 supersedes 边 + 旧节点 −0.5（原子幂等，返回 ok/duplicate）。

    FF 决策在 Python 侧（compute_delta 单点，固化锁定语义自带）；图脚本只做
    存在性检查与落库。
    """
    phi = ff.read_phi(client, gname, space_id=space_id, node_key=old_node_key)
    new = ff.compute_delta(
        phi,
        delta=ff.DELTA_CONFLICT,
        touch=True,
        counter_key="conflict_count",
        n_now=n_now,
        config=ff_config,
    )
    result = (
        client.submit(
            _APPLY_CORRECTION_SCRIPT,
            {
                "gname": gname,
                "spaceId": space_id,
                "newKey": new_node_key,
                "oldKey": old_node_key,
                "sNew": new.s,
                "nStar": str(new.n_star_cached),
                "nNow": str(n_now),
                "locked": phi.consolidated_at is not None,
            },
        )
        .all()
        .result()
    )
    if "duplicate" in result:
        return "duplicate"
    if "ok" not in result:
        raise RuntimeError(f"纠错施加未返回 ok/duplicate：{result}")
    return "ok"


def process_corrections(
    client: Client,
    ex_session: Session,
    *,
    gname: str,
    space_id: str,
    n_now: int,
    ff_config: ff.FFConfig = ff.DEFAULT_CONFIG,
) -> CorrectionsStats:
    """单 space 一轮：扫 EX 纠错事件，逐条幂等施加 supersedes 边 + −0.5。"""
    stats = CorrectionsStats()
    for event in list_experience_events(ex_session, space_id=space_id):
        if not event.ref_conflict:
            continue
        new_key = _find_node_by_refex(client, gname, space_id=space_id, ref_ex=event.event_id)
        if new_key is None:
            stats.pending += 1  # 新节点尚未入图（M15 入链前常态），下轮再处理
            continue
        try:
            outcome = apply_correction(
                client,
                gname,
                space_id=space_id,
                new_node_key=new_key,
                old_node_key=event.ref_conflict,
                n_now=n_now,
                ff_config=ff_config,
            )
        except KeyError:
            stats.pending += 1  # 旧节点不在图中（可能已归档），下轮再处理
            continue
        if outcome == "ok":
            stats.applied += 1
        else:
            stats.duplicate += 1
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="纠错处理器（M7）：扫 EX 纠错事件，幂等建 supersedes 边 + 异步 −0.5"
    )
    parser.add_argument("--space", help="只处理指定 space（缺省处理全部 space）")
    args = parser.parse_args()

    client = gremlin_client()
    ex_cluster = ex_cassandra_cluster()
    ex_session = ex_cluster.connect()
    redis = redis_client()
    try:
        if args.space:
            spaces = [args.space]
        else:
            store = ExKeyspaceControlPlaneStore(ex_session, StaticControlPlaneStore.local())
            spaces = store.list_spaces()
        for space_id in spaces:
            stats = process_corrections(
                client,
                ex_session,
                gname=space_id,  # 图名 = space_id（M5 定案约定）
                space_id=space_id,
                n_now=n_now(redis, ex_session, space_id=space_id),
            )
            print(
                f"[{space_id}] applied={stats.applied} "
                f"duplicate={stats.duplicate} pending={stats.pending}"
            )
    finally:
        client.close()
        ex_cluster.shutdown()
        redis.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
