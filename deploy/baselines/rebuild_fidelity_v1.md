# M7 全保真档重放演练报告 v1（2026-08-14，灾难恢复能力证明 / 准出硬项）

数据源：阶段 A 真实冒烟数据（smoke_main，20 事件：18 常规 + 2 纠错，DeepSeek 真实打分、
text-embedding-v4 真实向量）。演练前补充造态：hobby_trail reinforce×3 + FS sweep 固化
（consolidated_at 置位、n_star_cached=LONG_MAX）、rel_girlfriend reinforce×2 未达阈值。
驱动：`scripts/rebuild_fidelity_drill.py`（snapshot / destroy-rms / diff 三相位，可重跑）；
快照 `var/smoke/{before,after}.json`，diff 明细 `var/smoke/diff.json`。

## 1. 演练流程

1. 快照：顶点 20（全字段）、边 21（19 temporal + 2 supersedes）、rms_vectors 20 文档、归档 0。
2. 销毁 RMS 侧（红线 5 顺序：close + removeConfiguration → DROP KEYSPACE → 删向量文档）：
   20 顶点/21 边/20 向量全部清除，耗时 **7.5s**。
3. 从 EX 完整重放（全保真档，`ex_scoring_s_resolver` 读 scoring_result details）：
   `python -m lethefield_rms.rebuild smoke_main`，**耗时 16.4s**（冷图创建 7.4s + 重放落库 ~9s，
   20 事件规模）。无 `rebuild_scoring_missing`（20/20 打分元事件齐全）。
4. 字段级 diff + 服务恢复验证。

**RTO 首测：约 24s（销毁不计入则为 16.4s）**；修订 27 修复后重跑（本报告 §2 数据
以重跑为准）：重放 **21.9s**——20 事件微型 space 的图侧恢复；
检索面恢复 = ES 快照运维前提（修订记录第 25 条，实证见 §3）。规模外推无依据，
留待混沌演练实测。

## 2. diff 结果

校验边界按**修订记录第 25 条**：M7 全保真档的"RMS 全部状态"= 图结构 + 状态场字段
（含 s 与 δ 推导链、固化态、supersedes、node_key 关联）；rms_vectors 不属重放范围，
向量侧只验 node_key 关联语义可重建（热节点向量灾难恢复 = ES 快照/备份运维前提，
runbook 见 `运维-runbook-ES快照备份恢复-v0_1.md`）。

| 类别 | 结果 |
|---|---|
| 顶点字段级（content/τ/ref_ex/s/n_created/n_last_touched/n_star_cached/三计数器/A_i/固化态存在性） | **20/20 精确相等，零差异** |
| temporal 边 19 + supersedes 边 2 | 完全一致 |
| 归档快照 | 0 = 0（本数据集 grace_n 未达，未覆盖，见缺口登记） |
| 向量 node_key 关联 | 图侧 node_key/ref_ex 全部保真，向量重关联语义可重建 ✓（重嵌入不属本次演练） |

**δ 重放链验证（工单关注点）**：两条被纠错旧节点重放后 s=0.000 与销毁前**精确一致**——
推导链完整（scoring_result details 原始 s → ref_conflict 事件重放 −0.5 → clamp 0），
conflict_count=1 一致；reinforce 链（三次 +0.2）与固化态（n_star_cached=LONG_MAX、
consolidated 存在性）同样精确复现。

**零差异的达成路径（修订记录第 27 条）**：首轮演练曾发现 home_old `n_last_touched`/
`n_star_cached` ±1 差异——直播路径 `apply_correction` 原取处理时刻（当轮全局 n_now），
重放取纠错事件自身 `event.n`；处理时刻不被任何契约记录、随处理器调度漂移，不可能是
规范状态。定案修复 = touch 时刻改取 `event.n`（事件时刻语义），本轮重跑验证直播值
（n_last_touched=19）与重放值**精确相等**，保真校验无容忍例外。

## 3. 发现项

1. **检索面灾难恢复 = ES 快照运维前提（修订记录第 25 条定案，本次演练提供实证）**：
   rms_vectors 文档销毁后实测——retrieve 两路全灭（kNN 与关键词都读 rms_vectors 文档），
   重建前命中的 `我现在住在哪里` 返回空；图侧重放完成后检索依然为空，直到向量经
   ES 快照恢复。**这就是"为什么 ES 快照是必备项"的实证**：没有 rms_vectors 快照备份，
   图结构恢复得再完整检索面也不可用。召回退化形态 = 静默空结果（无报错、无部分命中），
   运维侧若无快照巡检，该故障面不可见。
2. 写入链自愈验证通过：重建后 record n=21 正常摄入，SS 打分 + writer 建点完成，
   `writer_n_gap_total=0`（NTracker 从重建图 max n_created=20 正确播种）、DLQ=0。

## 4. 缺口登记

- 归档重放保真未覆盖（本数据集最大 n=20 < n_star+grace_n=60）：归档快照 v_i 携带已由
  M13 集成测试覆盖；完整归档重放演练需 n≥60 数据集，建议并入混沌演练计划。
- consolidated_at 时间戳按定案只保存在性（重放置执行时刻），diff 口径已对齐。
