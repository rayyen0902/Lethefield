# 真实外部依赖端到端冒烟报告 v1（2026-08-14，Mac 本机）

- 环境：`make reset` 清卷全新栈；API :8000 + SS worker(:9105, DeepSeek `deepseek-v4-flash`) + writer worker(:9106, DashScope compatible-mode `text-embedding-v4` 1024 dims) 均真实外部依赖。
- 驱动脚本：`scripts/smoke_real_llm.py`（可重跑）；原始数据 `var/smoke/raw.json`、状态 `var/smoke/state.json`。
- writer 启动校验：`ensure_vectors_index(dims=1024)` 与 `rms_vectors` mapping（`v`: dense_vector 1024/cosine）一致，正常接管索引。
- 测试身份：IS 真实链路开户 `smoke_acct` → 开通 `smoke_main`(hot) / `smoke_other`(cold) → 分 space 签发 JWT（含 debug scope，--internal 渠道）。

## 1. 写入链（API → EX → Pulsar → SS → writer）

- 主 space 18 条中文记忆事件（爱好/工作/情绪/人际/计划/生活事实六类）+ 2 条 flag_conflict 纠错；对照 space 3 条诱饵。共 23 条全部完成：SS `llm_calls_total{ok}=23`、`dlq_total=0`、`degraded=0`；writer `embed_calls_total{ok}=23`、`dlq_total=0`；EX scoring_result=20、rms_vectors=20（主 space）。
- 纠错（corrections 单轮 `applied=2 duplicate=0 pending=0`）：supersedes 边 2 条（新→旧），旧节点 `s=0.0`（−0.5 后 clamp）、`conflict_count=1`、即时进入归档视界（n_star_cached=n_now）。

## 2. 检索结果明细（query_text + query_vector 双路，n_now=20）

| 查询 | 期望 | 实际 | 判定 |
|---|---|---|---|
| 我喜欢什么户外运动？ | 命中徒步 | 仅返回 home_new（s_eff=0.433） | ✗ |
| 我女朋友是做什么工作的？ | 命中小雨/插画 | 仅返回 home_new | ✗ |
| 我有什么旅行计划？ | 命中日本/京都 | 仅返回 home_new | ✗ |
| 量子计算的最新进展是什么？ | 不命中 | 空 | ✓ |
| 我现在住在哪里？ | 命中新地址 | home_new（中关村），旧望京未返回 | ✓ |
| 我开什么车？ | 命中 Model 3 | 空（car_new s_eff=0.233 < θ） | ✗ |
| 我有什么药物过敏？ | 命中青霉素 | 仅返回 home_new | ✗ |

跨 space 抽查：主 space 凭证查对照 space → 403 forbidden_space ✓；对照 space 内同向量检索零泄漏（无任何主 space 内容）✓。

## 3. s 值分布与六维样例（EX scoring_result details，全保真来源）

- 20 条：min 0.117 / max 0.483 / mean 0.298；degraded 0。
- 样例：
  - emo_anxiety「服务器半夜宕机很焦虑」s=0.433 `{er:0.8, e:0, i:0.7, g:0.8, n:0.3, c:0}`
  - home_new「搬到中关村两居室」s=0.483 `{er:0.4, e:0.1, i:0.9, g:0.2, n:0.6, c:0.7}`
  - work_investor「投资人季度汇报」s=0.317 `{er:0.3, e:0, i:0.6, g:0.8, n:0.2, c:0}`
  - misc_movie「星际穿越看三遍」s=0.167 `{er:0.3, e:0, i:0.4, g:0, n:0.2, c:0.1}`

## 4. token 消耗与成本估算

- SS（DeepSeek）：23 次调用，prompt 5987 + completion 2537 = 8524 tokens（≈370 tok/事件）。
- embedding（DashScope）：写入 23 次 317 tokens + 查询侧 8 次 50 tokens = 367 tokens。
- 成本：刊例价未配置进系统（`LETHEFIELD_SS_PRICE_*` 缺失，M14 已登记）。按 DeepSeek 公开刊例量级（输入 ¥1–2/百万、输出 ¥8/百万，待核实）本次冒烟 SS 侧约 ¥0.02–0.04；embedding 侧不足 ¥0.001。单次记忆写入的 LLM 成本量级 ≈ 0.1–0.2 分人民币。

## 5. 问题清单与分类

**配置/代码 bug：无。** 全链路（record → SS → writer → corrections → retrieve）机械行为全部符合既有定案；驱动脚本自身一处 KeyError 已修。

**观察项（打分/召回质量，未调参，升级确认）：**

0. **【种子期标定第一优先级基准】** `car_new` 在 Δn=0 即被 θ 过滤的 case（s=0.233 < θ_base=0.3，
   "我开什么车？"返回空）：参数标定后必须复验通过，作为标定是否达标的硬判据之一。

1. **λ=0.16 占位下有效记忆窗只有 2–3 个事件**：n=9 的女朋友事件 Δn=11 → s_eff=0.433×e^(−0.16×11)≈0.074，被 θ_base=0.3 过滤。除最新 1–2 条外全部记忆对检索不可见，7 查询仅 2 个符合预期。λ/θ_base 均为 §20 占位待标定。
2. **DeepSeek 对日常个人记忆打分系统性偏低**（mean 0.298，20 条中 12 条 s<0.3）：与 M14 稳定性验证的样例集（mean 0.365）相比更低；s<θ_base 的记忆即使 Δn=0 也直接被过滤（car_new 即此例，q_car 返回空）。六维权重均权占位待标定（§20）。
3. **纠错覆盖的正确性在本参数下无法与衰减区分**：旧节点 s 已被 −0.5 清零，即使无 supersedes 降权也不会返回；supersedes 的检索期覆盖语义需在标定后的参数下复验。
4. 召回验证的"应命中"判定依赖关键词比对（脚本侧简易判定），正式标定需要标注集。

**结论**：冒烟目的（真实依赖端到端链路可用性）达成；检索质量不达标项全部可归因于 §20 占位参数，非代码缺陷，按纪律不调参、升级为标定输入。
