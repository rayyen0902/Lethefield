# Lethefield / RMS — 1.0 开发文档

**依据**：《Lethefield — RMS 设计文档》v1.7（决策层封版）
**文档性质**：开发执行文档（非设计文档）——只翻译"做什么、怎么做、怎么算做完"，不重复设计论证过程；论证过程与已否决方案见原设计文档，本文档只保留结论。
**目标读者**：负责 1.0 阶段开发的工程师
**使用方式**：按"模块"领取任务；每个模块包含【目标】【前置依赖】【任务清单】【接口 / 数据结构】【验收标准】【明确不做】五项，验收标准是唯一的完成判据，任务清单中未列出的自由发挥空间**不需要、也不允许**自行决定架构性选择——如遇设计文档未覆盖的分支，先升级确认，不要自行拍板。
**版本**：v1.2（评审修订版）

**v1.2 修订记录**（相对 v1.1）：
1. 新增模块 M17（运维操作面）：显式定义 1.0 运维形态为 Grafana（读）+ 运维 CLI（写）+ 决策留痕表单（痕）；M9/M10 的人工触发点此前无责任模块，本模块收口；Web 管理后台明确不做，归 2.0。
2. M6 升级确认定案：sweep 活跃 space 列表来源定为 `ControlPlaneStore` 抽象方法，过渡实现读 EX 集群元数据按 `ex_*` 命名推导，M9 落地后切映射表（详见 M6 忽视惩罚实现规则）。
3. 固化机制升级确认定案：图 schema 加第 17 个顶点属性 `consolidated_at`（存在即固化态，兼作审计时间戳）；M4 两处 θ 硬过滤对固化节点旁路、`n_star_cached` 置 LONG_MAX；固化后 ±δ 不改 `s`（计数器照计）；固化节点被纠错照常建 supersedes 边、固化态 1.0 不解除（详见 M2/M4/M6）。
4. 归档冷存储载体升级确认定案：本 space RMS keyspace 内专用表 `archived_nodes`；否决共享 keyspace 逻辑分区、EX 集群归档表、PG 三方案；重放重建与迁移校验须覆盖该表（详见 M6 节点生命周期）。
5. 归档宽限期度量升级确认定案：事件距离 `grace_n`（`n_now ≥ n_star_cached + grace_n`），禁用墙钟——记忆动力学只在 n 域运算；静默 space 资源回收归封存层/tier 机制（详见 M6、§20）。
6. SS 打分可重建性升级确认定案：M14 打分结果以 `scoring_result` 元事件回写 EX `meta_events` 表（不推进 n），解决"LLM 打分不可重放、RMS 重建时原始 `s` 丢失"的保真缺口；M7 重建验收分两档——M14 落地前 `s` 走注入的 `s_resolver` 占位常数、不含 `s` 保真，落地后切换真实来源、升级全保真（详见 M7/M14）。
7. 销毁指令接口形态升级确认定案：训练 tenant 持久化控制 topic（Pulsar），生产者等 broker ack、失败即标记步骤失败并告警留痕，consumer backlog 纳入监控，指令 schema 单点管理（详见 M10 注销第 4 步、M11 授权撤回与删除权联动）。
8. 迁移演练升级确认定案：分两档——本地档（RMS 真跨 Cell 走 compose `cell2` profile、EX 同集群 pipeline 演练）+ 准出档（临时扩容环境补跑含 EX 跨集群流式传输的完整演练）；否决常驻第二 EX 集群（OOM 风险换偶发需求）与"本地不演练"（不满足验收）（详见 M10 验收标准）。
9. Dead Man's Switch 落地形态升级确认定案：双路分离——集群级 probe topic 探针（page 级，禁止向 space namespace 发探针污染数据面）+ space 写入新鲜度（observation 级；「活跃」= 过去 W 窗口有摄入或 hot/premium tier；W 与超窗阈值待标定）（详见 M10、§20）。
10. R3 检测机制升级确认定案：关联式实现——retrieve 发射最小化召回明细日志事件（`space_ref` 哈希 + `node_key` 列表 + θ 统计，不含原文），授权拦截后过境训练 topic，worker 按 `node_key` + `W_r3` 时间窗与 ④ 路纠错对关联，命中才产样本；未经召回的纠错不计入 R3；否决"纠错即样本"简化（污染标定闭环）（详见 M11）。
11. 决策留痕表结构补齐（§11.3 既定要求）：新增 `agent_suggestion` / `outcome`（`accepted|modified|rejected`）/ `escalation_type`（可空）三字段；R1/R2 在提交路径判定喂训练 topic ① 口；否决"只进训练样本、不动表"（留痕库审计面缺位）（详见 M0 任务 5）。
12. 图指标观测口径升级确认定案：`graph_open_duration` 走客户端近似（显式 open 点埋点，服务漂移告警）；`graph_lru_cache_hit_ratio` 改离线推导代理口径（闲置后首请求 + Stage 耗时异常占比，复用召回明细日志事件，零新增埋点）；否决 JMX sidecar（未验证 + 过重）与"登记缺口延后"（标定消费方断粮）（详见 M12 系统指标表）。
13. `n_now_lag_seconds` 口径升级确认定案：原"缓存滞后"语义已被写穿实现消灭；改名 `ex_last_write_age_seconds`（space 距最近成功写入时长，数据源 `ex:last_write:{space}`），DMS 巡检进程发射聚合 max/p95 gauge（禁 space_id 标签）；否决"缓存 vs EX 差值"（测量不存在的失败模式）与静默延后（详见 M12 系统指标表）。
14. M11 ③ 入料口收口时机定案：M12 内一次收口为定案形态（过滤器读日志管线），API 进程内闸门与训练 topic 发布代码整体删除，新增 M12 验收项锁定（详见 M12 验收标准）；否决"只建管线、闸门存续"（双写路径延寿）。
15. 红线 3 Redis 逐出豁免 + n 权威值配套定案：现存 Redis 键均为小键、逐出无内存收益，`ex:n` 逐出会破坏 n 分配——豁免 Redis 逐出（不配 maxmemory-policy 逐出，防误伤权威键）；同时修正 M6 文档分叉——`ex:n` 定案为权威值（非缓存、无惰性重建），配套 AOF 持久化 + Redis n vs EX MAX(n) 一致性巡检（回退即告警）；否决惰性重建改造（动冻结写路径换零收益）与策略占位（无对象可逐出）（详见 M6、M13）。
16. 索引形态一致性订正 + 归档向量处理定案：订正 v1.1 修订记录第 5 条（原表述丢失 mixed/vector 区分）——**每 space 独立 mixed index 可冻结；向量维持共享 `rms_vectors` + routing（有意架构选择：per-space 向量索引 = 索引数随 space 爆炸），非待议**；归档时同步删除 `rms_vectors` 对应文档、归档快照携带原始 `v_i`（embedding 不可重放）（详见修订记录第 5 条、M6 归档行）。
17. 红线 2 配额落地形态定案：`QuotaConfig`（全局默认 + tier 可选覆盖）；写入侧硬校验落现有 writer/向量路径（不等 M15）、图 count 短 TTL 近似执行（超发有界）、查询侧补返回节点数硬上限；`QuotaExceeded` → API 映射既有 429（不新增契约码）；ES 存储字节走指标监控不做硬校验（详见 M13 红线 2 行）。
18. 红线 1 常驻 worker 合规定义定案：经 ControlPlaneStore 枚举、逐 space 独立处理的常驻 worker 本身合规；豁免三要件（控制面枚举 / 逐 space 处理 / 批间节流）+ 机器可读登记（扫描器只认登记不认注释）；新增批处理入口 argparse 强制 `--space(s)`；否决"所有入口强制 --spaces"（废掉 fleet 级 worker 的天职）（详见 M13 红线 1 行）。
19. 契约 1 首次演进（对第 6 条"不改表结构"表述的修正）：`meta_events` 加可空 `details` 列（text/JSON，按 meta_type 分型、schema 单点；既有类型置空行为不变），承载 `scoring_result` 六维原始值；自此明确契约演进规则——只允许可空加列式兼容演进，改语义/改键/改摄入路径视同契约修改走完整升级确认；否决 `scoring_results` 专表（打碎元事件信封统一性、M7 重建多读一表）（详见 M14）。
20. EX→Pulsar 生产侧定案 + M5 红线修订：经验事件由 API 摄入路径在 EX 落库确认后发布（有限重试、失败不阻塞返回、最终失败 page 告警）；M5"API 不依赖 Pulsar"红线修订为"**同步返回路径**不依赖 Pulsar"（立法本意是保护同步路径可用性，producer 依赖显式登记，集成测试断言同步更新）；消费侧自愈靠 n 连续性校验（M14/M15 缺口告警 + 按 n 区间从 EX 补偿重放），不新立轮询 relay 组件（详见 M10 Pulsar 实现要求、M15）。
21. M14 打分客户端形态定案：OpenAI 兼容 HTTP 直连（httpx 复用、零新 SDK、provider 中立、env 配置）；输出解析失败走降级规则；调用量与估算成本打标定线指标（详见 M14）。
22. M14 降级规则分级定案：缺 1 维 → 中性值（默认 0.5 可配）+ degraded 标记回写 EX（未来可识别可重打分）+ 指标计数；不可解析或缺 ≥2 维 → 失败路径（重试 → DLQ）；`SS_DEGRADE_POLICY` 配置开关默认 `neutral_mark`；否决"缺维即整单重试"（LLM 抖动放大 DLQ，拿可用性事故治精度小问题）（详见 M14 降级规则）。
23. M15 写入链实施定案（信封口径 + node_key 冻结 + embedding 一致性）：① `ScoringResult` 信封只作触发与 `s`/`node_key` 来源，`c_i`/`τ_i`/`A_i` 一律按 n 反查 EX（点查/区间查新接口）；信封与 EX 记录一致性校验（`envelope.node_key == node_key_of(event_id)`、`envelope.space_id` == topic namespace）不符 fail-closed → DLQ，EX 查无该 n = 上游不一致 → nack → DLQ + page；② node_key 规则冻结为 `node_key_of(event_id) = "ev_{event_id}"`，SS/writer/rebuild 三处单点对齐，rebuild"过渡约定"注释同步转正；③ 时序边前序 = 图内 `n_created < n` 的最大者，归档缺口不跨接（与 M7 重建"只建双端在热图"同规约，理想链由重放修复）；④ embedding 一致性规则：共享 `rms_vectors` 内的向量必须出自同一 embedding 模型（同 dims 不同模型语义不兼容），模型变更 = 该索引向量全量重建，选型与变更进决策留痕；⑤ n 缺口补偿按 n 区间从 EX 读事件 + `scoring_result` details 取 `s`（全保真），details 缺失 = SS 尚未打分 → 跳过等 SS 侧补偿重发（正常路径幂等兜底）（详见 M15）。
24. M16 凭证体系定案（契约 3 首次演进 + 吊销机制选型）：① 契约 3 兼容演进——JWT 新增标准注册 claim `jti`/`exp`/`iat`（契约 3 四字段结构不变，验证侧对无 `jti` 的旧 dev token 跳过吊销检查，向后兼容；契约 3 自此沿用契约 1 演进规则：只加不改）；② 吊销机制 = jti 吊销列表（PG `is_credentials.status`，API 验证侧逐请求检查、checker 异常 fail-closed 传播不静默放行）+ 有限时效（默认 TTL 24h，`LETHEFIELD_IS_TOKEN_TTL_SECONDS` 覆盖；1.0 不做刷新机制，重签发即刷新）；③ `debug` scope 签发侧闸门 fail-closed（非 internal 拒签）；④ scope 白名单单点化（`libs/clients/credentials.py`，`api.auth` 与 `is.tokens` 同源引用，禁双拷贝对齐注释）；⑤ 账号/归属/凭证三表落 PG（`is_accounts`/`is_space_owners`/`is_credentials`）；⑥ 空间创建顺序：校验账号 → `validate_space_id` → provision 成功后才写归属行（无半开通状态）（详见 M16）。
25. M7 重建验收边界定案（检索面恢复归属）：全保真档的"RMS 全部状态"明确为**图结构 + 状态场字段**（顶点字段级、时序/supersedes 边、固化态、`s` 值与 δ 推导链、向量关联 `node_key`）；`rms_vectors` 向量属检索加速层、不可从 EX 重放（第 16 条已确立 embedding 不可重放，归档节点靠 `archived_nodes` 快照带原始 `v_i`），热节点向量的灾难恢复 = **ES 快照/备份运维前提**——1.0 不改代码，ES 快照备份纳入 M17 运维 runbook 必备项（快照存在性纳入巡检）；否决重放后按 content 自动重嵌入（与"重算漂移"定案张力 + 模型版本漂移使重嵌入向量与索引存量语义不兼容，见第 23 条④；应急人工重嵌入留 2.0 候选，届时走设计层修订）；否决热节点向量快照副本（存储翻倍换 ES 快照已覆盖的能力）（详见 M7、§19 准出清单）。
26. M17 运维操作面实施定案：① 形态 = 新包 `ops/ops_cli`（lethefield-ops-cli）统一收口全部人工触发点，scheduler/training/is 既有 CLI 保留为底层入口（测试与巡检依赖），ops_cli 在其上加"强制绑定 + 自动留痕"操作面；② 命令九条：`space status/destroy/set-tier`、`migrate rebalance/to-cell/evacuate`（迁移三类触发入口）、`auth revoke`、`cell watermark/register`，每条带**必选** `--space`/`--cell` 绑定（evacuate 也是显式 space 列表，无"对全部执行"形态），静态检查 = parser 内省单测（`test_no_global_commands.py`）；③ 留痕包装单点 `audit.run_with_audit`：PG 预检 fail-closed（留痕库不可达 → 拒绝执行，处置类与留痕的原子要求由此满足）、`outcome` 恒 `accepted`（枚举语义 = 人类对建议的处置结果，ops 决策无 agent 建议，不拉伸；执行成败记 `context.result`）、业务已执行但留痕写入失败 → 退出码 2 + 人工补录提示（不静默）；④ 操作人解析 `--operator` > env `LETHEFIELD_OPERATOR` > OS 用户；⑤ tier 升降 = `MappingTableControlPlaneStore.update_space_tier`（只改 tier 字段，ABC 冻结六方法不动）；⑥ 新 Cell 筹备 = `cell register` 登记映射行（endpoints 必含 `cassandra`/`es` 两键，INSERT 覆盖写幂等；Cell 基础设施由运维自备，本命令只收口"可参与调度"触发点）（详见 M17）。
27. 冲突 δ touch 时刻语义定案（事件时刻为规范值，推翻"文档化接受偏差"建议）：直播路径 `apply_correction` 原取处理时刻（处理器当轮全局 `n_now`），重放取纠错事件自身 `event.n`——处理时刻不被任何契约记录、依赖处理器调度节奏，同一 EX 流在不同调度下产生不同直播值，不可能是规范状态（规范状态 = 可从 EX 确定性推导）。定案：`apply_correction` 的 touch 时刻改为纠错事件的 `event.n`（事件时刻语义），直播与重放天然一致，保真校验**精确相等、无容忍例外**；与 reinforce 路径（EX 元事件显式记录 `n_at_event`，直播重放天然一致）对齐为同一范式。与 M7"理想化 sweep"定案**分族**：sweep 节奏是后台调度产物（不可记录，接受偏差是没得选），冲突 touch 是事件驱动写入（`event.n` 在手边，可免费确定，不接受偏差）；否决"文档化接受偏差"（容忍例外的保真校验弱于精确相等，例外随时间扩大、稀释灾难恢复与迁移校验的证明力）与"EX 记录处理时刻"（把调度非确定性烘进永久状态）（详见 M7 纠错路径、§19 重放准出项）。

**v1.1 修订记录**（相对 v1.0，评审发现的问题修复）：
1. 新增模块 M0（工程地基）、M14（SS 显著性打分服务）、M15（写入链 worker）、M16（IS 简版）——v1.0 中 SS、写入链、IS 三处只有零散要求、无责任模块，属遗漏。
2. M9 的开通/注销流程与 M10 矛盾（顺序不一致、注销缺训练管线广播），统一以 M10「三存储生命周期流水线」为最终版，M9 只保留 Cell 特有内容。
3. Cell 落地时机定案：**代码按 Cell 最终形态实现**（数据访问层走 `ControlPlaneStore` 抽象），**部署按最小规模起步**（单节点，3 节点为生产参考配置，非 1.0 起步要求）。
4. M12 引用修正：运维日志 ES 由 M1 部署（v1.0 误写为 M11）。
5. M13 红线 3 适配 Cell 形态：**每 space 独立 mixed index** 可索引级降配/冻结；**向量索引维持共享 `rms_vectors` + custom routing**（有意架构选择：per-space 向量索引 = 索引数随 space 数爆炸，数十亿 space 远景下不可行；其冷热控制靠 M6 归档同步删除向量文档）。（v1.0 沿用旧的"共享 index + custom routing 下 ES 无法索引级冻结"语境；本条 v1.2 订正——原表述丢失 mixed/vector 区分，与 §14 红线 3 行末口径矛盾，以本口径为准）

---

## 0. 总览

### 0.1 服务清单与 1.0 范围

| 服务 | 全称 | 1.0 是否开发 | 说明 |
|---|---|---|---|
| IS | Identity Service | ✅ 开发（简版） | 账号级身份，鉴权凭证签发 |
| EX | Experience | ✅ 开发 | 全量事件记录，不可变 source of truth |
| SS | Salience Service | ✅ 开发 | 六维度显著性评分 |
| RMS | Relational Memory Service | ✅ 开发（本文档主体） | 多图记忆基座 |
| FS | Forgetting Function（服务） | ✅ 开发 | 遗忘函数计算 + sweep worker |
| MS | Memory Surface | ❌ 不开发 | 延期至 2/3 期，本次不立项、不预留接口 |
| ES | Growth Service | ❌ 不开发 | 已挂起，本次不立项、不预留接口 |
| 租户调度器 | Tenant Scheduler | ✅ 开发 | Cell 架构控制面 |
| 训练数据管线 | Training Pipeline | ✅ 开发（最小集） | §12.4 定义的最小实现 |

**阶段边界（严格执行，不要提前做 2.0 的事）**：

- **阶段 0/1（本次开发范围）**：自有小规模集群，技术落地验证。包含：§7 技术栈、§15 存储物理隔离目标架构在自有集群落地、§17 Cell 架构、§18 EX/Pulsar 归属、§9 MCP/SDK 接口、§13 记忆维护机制、§19 可观测性最小集、§12.4 训练管线最小集。
- **阶段 2（不在本次范围）**：第三方服务商部署规范与一致性认证套件（原设计文档课题 3）、per-space 加密（服务商可选特性）、封存层自动化完整版、Premium 全常驻档、训练数据服务商场景授权结构。**开发人员不需要为这些预留特殊扩展点之外的工作量**——只需保证接口契约稳定（见各模块"面向未来的兼容点"）。

**Cell 落地时机（v1.1 定案，统一口径）**：代码按 Cell 最终形态实现——所有存储访问必须经过 `ControlPlaneStore` 抽象与 space→Cell 映射，禁止出现"绕过映射直连默认集群"的快路径；部署按最小规模起步——1.0 起步为单节点各组件（见 M1），3 节点为生产参考配置而非起步要求。即：**架构一分不减，规模按需起步**。

### 0.2 架构不变量（贯穿所有模块，任何实现都不能违反）

1. **检索永远限定单一 `space_id` 内**，不存在跨空间检索。任何代码路径出现"跨 space 联查/扫描"即为 bug。
2. **EX 是唯一不可变 source of truth**；RMS 的一切状态必须可从 EX 重放重建。
3. **禁止全局广播 / 跨 space 全集群扫描**（红线 1）：运维后台、批处理、数据分析任务入口必须绑定显式 space 列表。
4. **FF 衰减不物化**：存储只保存 δ 调整后的基准显著性 `s`，`s_effective` 一律读取时现算。
5. **纠错走事件入链**，不设硬失效标志，不做旧节点的直接状态覆写。
6. **删除权粒度 = 整 space**，不支持空间内单条事件物理删除。

### 0.3 M0 — 工程地基（先于一切业务模块启动）

**目标**：在任何业务模块开工前，把"能跑起来、能验证、能留痕"的地基打好，避免各模块各自搭脚手架造成后期整合返工。

**任务清单**：
1. **monorepo 初始化**（Python 统一）：目录结构、依赖管理、lint/测试基线、CI 流水线骨架。
2. **共享库三样**（所有模块强制复用，禁止各写一套）：
   - 结构化日志事件 schema（M12 日志管线的原料，space 粒度明细的统一格式）；
   - 指标 registry 封装（命名规则 `lethefield_<域>_<名称>_<单位>`、标签白/黑名单在代码层强制，见 M12）；
   - 存储与 Pulsar 客户端封装（连接管理、`ControlPlaneStore` 抽象接口定义——实现随 M9 落地，接口在 M0 冻结）。
3. **docker-compose 单节点全栈**：Cassandra（Cell 用 + EX 用两实例）+ ES（图索引/向量用 + 运维日志用两实例）+ Pulsar + Redis + PostgreSQL，一键起栈用于开发与 CI。
4. **spike q1–q4 脚本转 CI 集成基线**：spike 已验证的四断言（高分召回/低分过滤/衰减过滤/跨空间隔离）移植为回归测试，CI 必跑（见 `spike/SPIKE_REPORT.md`）。
5. **决策留痕表单与授权注册表最小实现**：§11.3 决策留痕机制从第一天可用（M11 入料口①的前提）；授权注册表空表先建（M11 授权拦截的前提）。留痕表字段必须覆盖 §11.3 既定要求（v1.2 定案）：在 `title/context/decision/rationale/decided_by` 基础上补 **`agent_suggestion`**（Agent 建议内容）、**`outcome`**（`accepted|modified|rejected`，人类对建议的处置结果）、**`escalation_type`**（§11.2 四类，可空）——R1（`outcome≠accepted`）与 R2（`escalation_type` 非空）在提交路径判定并喂训练 topic ① 口，留痕库本身即可审计（表单即标注界面，不新增标注工种）。

**验收标准**：
- [ ] 新机器 clone 仓库后，一条命令起全栈、跑通 CI（含 spike 移植的四断言回归），全绿。
- [ ] 共享库三样有独立测试；后续模块（M2 起）代码审查项包含"未重复造轮子"。
- [ ] 决策留痕表单可提交、可查询；授权注册表可增删查（为空也视为可用）。

**明确不做**：
- 不在 M0 实现任何业务逻辑（EX/RMS/SS/FS 一概不碰）。
- 不为多语言预留结构——1.0 只有 Python。

---

## 1. 技术栈与环境（确定性清单，不做二次选型）

| 组件 | 选型 | 版本参考（spike 已验证） | 备注 |
|---|---|---|---|
| 图计算引擎 | JanusGraph | 1.0.0 | 存储后端 Cassandra，索引后端 Elasticsearch |
| RMS 图存储 | Cassandra | 4.1 | 与 EX 使用的 Cassandra **物理隔离、独立集群池** |
| RMS 索引/向量 | Elasticsearch | 8.13.4 | mixed index + 独立向量索引（如 `rms_vectors`），与运维日志用 ES **物理隔离** |
| EX 事件存储 | Cassandra | 同上版本 | 独立集群池，per-space keyspace，与 RMS 集群池平行 |
| 消息/流转层 | Apache Pulsar | — | 每 space 一个 namespace；全局集群池，不随 Cell 分片 |
| IS 存储 | PostgreSQL | — | 标准关系型，强一致性 |
| 缓存层 | Redis | — | `n_now`、`n_star_cached` 等热点缓存 |
| 语言/运行时 | Python | — | 1.0 不做多语言分层 |
| API 层 | HTTP + JSON | — | MCP 协议为 JSON-RPC |

**强制配置项（spike 实测踩坑，禁止修改）**：

- `ids.authority.wait-time` **保持 JanusGraph 默认值，禁止调大**。这不是"ID 分配超时"，是 ID block 认领后的竞争确认睡眠；调大会导致每次 ID 申请固定睡眠该时长、等待方先超时，写入 100% 失败（`StandardIDPool.waitForIDBlockGetter` TimeoutException）。
- **禁止在 JanusGraph 实例在线时 DROP 其使用的 keyspace**。服务端 prepared statement 会引用失效表 ID（`Unknown CF`）并污染后续重建。任何涉及 keyspace 删除的流程必须先驱逐/停止对应计算实例（见 M9/M10 注销流程）。
- **节点时钟同步是硬性前提**，必须 NTP 硬化并监控时钟偏移。时钟跳变会让 Cassandra LWW 写入被"未来时间戳"的旧单元格静默吞掉、ID 分配挂起。**验收要求**：部署清单中必须包含 NTP 配置与时钟偏移告警项，缺失视为未完成。
- JanusGraph 1.0 全文谓词用 `TextP`（不是旧版 `Text.*`）；建索引用 TinkerPop `Vertex`（不是 `org.janusgraph.core.Vertex`，该类已移除）；Gremlin 绑定名避开保留字；新建 mixed index 停在 `REGISTERED` 状态即可投入使用，不需要等待其他状态。

---

## 2. M1 — 存储基础设施搭建

### 目标
按 Cell 架构（M9）+ EX/Pulsar 平行分片（M10）的最终形态，搭建可被调度器管理的存储资源池，而非搭一套单体集群后再改造。

### 任务清单
1. 部署 1 个初始 Cell（**最小形态：单节点 Cassandra + 单节点 ES**；3 节点为生产参考配置，非 1.0 起步要求，见 §0.1 Cell 落地时机），作为水位制调度的第一个 `open` Cell。
2. 部署 1 个初始 EX 集群（最小形态：单节点；生产参考为 3 节点高密度低规格机型），与 Cell 的 Cassandra **物理独立**。
3. 部署 1 套全局 Pulsar 集群（最小形态：单节点 standalone，含 broker + BookKeeper + 元数据存储一体），不与 Cell 绑定。
4. 部署独立的运维日志 ES 集群/索引，与 RMS 图索引/向量检索用 ES **物理隔离**（不同物理集群或至少不同用途隔离，禁止共用索引策略）。
5. 部署 Redis（缓存 `n_now`、`n_star_cached`，限流，会话状态）。
6. 部署 PostgreSQL（IS 账号/鉴权数据）。
7. 全部节点完成 NTP 硬化，接入时钟偏移监控告警。

### 验收标准
- [ ] 4 类存储（Cell 的 Cassandra+ES、EX 的 Cassandra、Pulsar、运维日志 ES）物理隔离可用 `describe cluster` / 连接串证明为不同集群实例。
- [ ] `ids.authority.wait-time` 为默认值，有配置巡检脚本可一键核验。
- [ ] 任意节点模拟时钟跳变（如 +60 分钟）触发告警。
- [ ] 容器/进程重启至可服务状态的时间有记录基线（spike 参考值 ~52s，用于后续容量规划对比，不是硬性指标）。

### 明确不做
- 不搭建 Milvus 或任何独立向量数据库。
- 不为 EX 使用 PostgreSQL。
- 不使用 NATS JetStream 或 Kafka 替代 Pulsar。
- 不使用 BerkeleyDB 作为 JanusGraph 存储后端。

---

## 3. M2 — RMS 图 Schema 实现

### 目标
落地节点（Event-Node）、四类关系图、φ_i 状态块的具体 schema。

### 数据结构：节点 `n_i`

| 字段 | 类型 | 说明 | 存储位置 |
|---|---|---|---|
| `c_i` | text | 事件内容 | JanusGraph 顶点属性 |
| `τ_i` | timestamp | 时间戳 | JanusGraph 顶点属性 |
| `v_i` | dense_vector | 稠密向量 | **Elasticsearch 独立向量索引**（如 `rms_vectors`），通过 `node_key` 与图顶点关联；kNN 检索带 `space_id` custom routing |
| `A_i` | structured | 结构化属性（含实体引用、`agent_actor_id`） | JanusGraph 顶点属性 |
| `ref_ex` | string | 指回 EX 原始事件 ID | JanusGraph 顶点属性，**RMS 与 EX 之间唯一关联机制**，逻辑引用不做物理共存 |
| `φ_i.s` | float | **基准显著性**（仅 δ 调整会写回；不是实时值，语义见 M3） | JanusGraph 顶点属性 |
| `φ_i.n_created` | long | 创建时的事件序号 | JanusGraph 顶点属性 |
| `φ_i.n_last_touched` | long | 最近一次被强化/冲突失效时的事件序号 | JanusGraph 顶点属性 |
| `φ_i.n_star_cached` | long | 缓存的遗忘视界预测值，后台定期刷新 | JanusGraph 顶点属性 |
| `φ_i.reinforce_count` | int | 累积强化次数 | JanusGraph 顶点属性 |
| `φ_i.conflict_count` | int | 累积冲突失效次数 | JanusGraph 顶点属性 |
| `φ_i.neglect_count` | int | 累积忽视惩罚次数（M6 用） | JanusGraph 顶点属性 |
| `φ_i.consolidated_at` | timestamp | **固化时间戳，存在即固化态**（兼作标志位与审计信息；v1.2 升级确认定案，第 17 个顶点属性） | JanusGraph 顶点属性 |

### 四类关系图 + 纠错边

| 图 | 边类型 | 建立规则 | 是否参与衰减 |
|---|---|---|---|
| 时序图 | 严格按时间戳排序 | 系统自动，immutable | 否，不参与任何衰减/剪枝 |
| 语义图 | `cos(v_i,v_j) > 阈值` | 由 consolidation worker 计算 | 否（方案 A：衰减只作用于节点） |
| 因果图 | `S(n_j\|n_i,q) > δ` | consolidation 阶段异步推断 | 否 |
| 实体图 | 事件→抽象实体节点 | consolidation 阶段异步推断 | 否 |
| **supersedes**（因果图特殊边） | `n_new --supersedes--> n_old` | 由纠错事件触发（M7） | 否，边本身不衰减 |

**遗忘粒度实现要求（方案 A，唯一实现版本）**：衰减只作用于节点 `s`；边只做存在性判定，不维护独立 φ_ij。**不要实现方案 B（按边独立衰减）**——设计文档明确列为未来升级路径，1.0 不做，出现 To B 需求前不要主动开发。

### 图数据库落地要求
- `s`、`n_last_touched` 等作为节点实时属性存储，检索按属性过滤（`WHERE s > θ_effective`），**不建外部状态表**。
- 后台 consolidation worker 负责：实体/因果边推断（慢路径）+ 顺带刷新到期的 `n_star_cached`。

### 验收标准
- [ ] 顶点 schema 包含上表全部字段，字段类型与索引方式（mixed index vs 独立向量索引）与表一致。
- [ ] 四类边 + supersedes 边可正确建立，时序图边不可被任何衰减/sweep 逻辑触碰。
- [ ] 向量检索验证：同一 space 的写入与查询均带一致的 `routing` 值，跨 space 查询返回 0 结果（零泄漏测试用例必须存在）。
- [ ] `ref_ex` 可 100% 追溯回 EX 原始事件（抽样校验脚本）。

### 明确不做
- 不为边单独维护 φ_ij 状态。
- 不把向量数据存进与全文/属性字段同一索引（同索引共存是备选方案，1.0 按独立索引实现）。

---

## 4. M3 — FF 计算引擎

### 目标
实现遗忘函数的现算逻辑与 δ 动态更新，且**衰减部分永不写回存储**。

### 核心公式（直接实现，不做变体）

```
FF(m, t, n, s) = s × e^(−λ × n × log(1 + t/t₀))
s_effective = s × e^(−λ·Δn·log(1+t/t₀))     其中 Δn = n_now − n_last_touched
n* = ln(s/θ) / (λ × ln(1+t/t₀))              遗忘视界预测
```

### 参数分层（实现时必须按此归属存放/传递，不能混层）

| 层级 | 参数 | 存放位置 |
|---|---|---|
| Memory-object runtime | `s`（基准显著性）、`n_last_touched` | 节点属性，随 δ 调整变化 |
| Agent-level constants | `λ`（衰减率）、`N_neglect`（忽视间隔） | 构建期按 agent 域固定配置，不在查询时改变 |
| Query-time controls | `ρ`（检索时动态参数，`θ_effective = θ_base / ρ`） | 每次检索请求传入 |

### δ 动态更新规则（实现为三条独立触发路径，不要合并成一个通用"打分接口"）

| δ | 触发条件 | 是否更新 `n_last_touched` | 同步/异步 |
|---|---|---|---|
| **+0.2** 强化 | `memory.reinforce` 调用（M5） | 是 | 同步直连 RMS，异步追加轻量元事件到 EX（fire-and-forget，见 M7） |
| **−0.5** 冲突失效 | 纠错事件被 consolidation 处理（M7） | 是 | 异步（consolidation 阶段施加） |
| **−0.1** 忽视惩罚 | FS sweep 周期触发（M6） | **否**（否则惩罚自我抵消） | 异步（sweep worker） |

### 实现红线
1. **衰减不物化**：任何后台任务不得把 `s_effective` 写回节点的 `s` 字段。`s` 只能被 δ 调整（+0.2/−0.5/−0.1）修改。
2. 检索流程（M4）必须在读取时对候选节点现算 `s_effective`，不得读取任何预计算的"当前显著性"缓存字段当作最终值（`n_star_cached` 只能用于前置粗筛，见下）。
3. `n_star_cached` 用途仅限图查询前置粗筛（`WHERE n_star_cached > $n_now`），**不参与实时计算**，用于排除明显跨越遗忘视界的节点以降低现算开销。

### 验收标准
- [ ] 单元测试：给定 `(s, n_last_touched, n_now, λ, t₀)` 组合，`s_effective` 计算结果与公式手算一致（含边界值：Δn=0、λ=0、s 触顶/触底截断）。
- [ ] 端到端测试：s=0.9 但 Δn=20 的节点在检索中被 θ 正确过滤（参考 spike 已验证场景，`s_eff≈0.10` 被过滤）。
- [ ] 存储层巡检：`s` 字段值只在 δ 触发时刻发生变化，两次 δ 触发之间的任意时间点读取 `s` 值不变（证明未被后台衰减写回）。
- [ ] `s` 值截断（clamp）到合法区间时，`ff_s_clamp_total{bound}` 指标（见 M12）正确计数。

### 明确不做
- 不做衰减结果的定期批量写回。
- 不在 λ1/λ2（检索相关性权重）之外，让 λ3（s_effective 权重）随查询意图动态调整——λ3 是域常数（agent-level constant）。

---

## 5. M4 — 检索流程（四阶段）

### 目标
实现 §5 定义的四阶段检索，**召回单元必须是带边子图，不是孤立节点列表**。

### 阶段定义（严格按顺序实现，不可合并/跳过阶段）

| 阶段 | 执行引擎 | 逻辑 |
|---|---|---|
| Stage 1（隐式，输入解析） | — | 解析 query，确定 `space_id`（必填，检索范围硬边界） |
| **Stage 2 锚点识别** | Elasticsearch | 语义检索（`dense_vector` + kNN，带 `space_id` custom routing）+ 关键词/属性检索，RRF 融合产出候选锚点集。**不将 `s` 塞入 RRF**。候选集产出后做一次后置硬过滤：现算 `s_effective`，低于 `θ_effective` 丢弃 |
| **Stage 3 自适应遍历** | JanusGraph（经 Cassandra） | 转移分数 `S(n_j\|n_i,q) = exp(λ1·φ(...) + λ2·sim(...) + λ3·log(s_effective(n_j)))`，软惩罚而非硬过滤；束搜索收敛后对最终候选池再做一次 `θ_effective` 硬过滤。**本阶段不再访问 Elasticsearch** |
| **supersedes 处理**（Stage 3 内嵌） | JanusGraph | 遇到带 `supersedes` 入边的节点，默认沿边重定向至取代者继续遍历；被取代节点不进最终候选池（除非查询显式要求追溯历史）；被取代节点仍参与衰减与归档，不因被取代而删除 |
| **Stage 4 叙事合成/token 预算** | 应用层 | 在 Salience-Based Token Budgeting 基础上叠加 `s_effective` 作为第二权重：两项分数都高 → 保留完整细节；相关但 s_effective 低 → 压缩为简短提示 |

### 前置粗筛
- 图数据库查询前使用 `WHERE n_star_cached > $n_now` 排除明显跨越遗忘视界的节点。
- **固化节点旁路（v1.2 定案）**：存在 `consolidated_at` 的节点不参与两处 `θ_effective` 硬过滤（Stage 2 后置、Stage 3 收敛后均跳过丢弃判定）；其 `n_star_cached=LONG_MAX`（M6 固化时置位）保证不被前置粗筛排除。固化≠失效，与 M7 禁失效标志红线不冲突——固化是有客观触发条件的生命周期状态，不是需要下沉到检索时的判断。

### ρ 旋钮作用范围（实现约束）
`θ_effective = θ_base / ρ` **只作用于**：Stage 2 硬过滤 + Stage 3 收敛后硬过滤。**不影响** Stage 3 遍历中的软惩罚权重 λ3。两个旋钮必须在代码上物理隔离（不能共用一个"相关性阈值"变量）。

### v0.1 暂定参数取舍（先按此实现，效果验证后再调整——调整需走参数标定流程，不是随意改代码）
1. λ3 为域常数，不随查询意图动态调整。
2. Stage 2 与 Stage 3 收敛后各做一次独立硬过滤，不合并为一次。

### 验收标准
- [ ] 检索返回结果结构为"带边子图"（含节点 + 时序/语义/因果/实体关系），不是扁平节点列表。
- [ ] Stage 3 期间无任何对 Elasticsearch 的调用（可用调用链追踪验证）。
- [ ] supersedes 节点重定向逻辑：构造 A→supersedes→B 链，检索默认返回 B（取代者），不返回 A；显式"追溯历史"参数下返回 A。
- [ ] ρ 变化只影响两处硬过滤阈值，不影响 Stage 3 遍历路径的软惩罚排序（对照测试：固定 λ3，只变 ρ，Stage 3 遍历顺序不变）。
- [ ] 跨 space 检索隔离：构造两个 space 各自写入相似内容，验证检索结果不互相污染。

### 明确不做
- 不做跨 space 的全局检索。
- 不把 `s` 直接塞进 RRF 融合分数。
- 不合并 Stage 2/3 的两次硬过滤为一次。

---

## 6. M5 — MCP / SDK 接口层

### 目标
落地 §9 定稿的四类操作接口，鉴权字段来源必须是凭证本身。

<注意：以前发现一个情况，就是Codex/Claude Code没有实时写入事件，你需要设计一下MCP和SDK，必须保证每次交互的时间必须要被上传，建议逻辑监视读取上传，不能依赖LLM主动上传
MCP的说明文档（就是每次交互发给LLM的说明书）是现在就做还是遗留到最后再不？这块需要一些技巧，不然LLM调了Codex/Claude Code 的原生记忆系统不主动调我们，所以这个板块需要后期独立做，要给LLM一个牛逼的Lethefield操作说明书>

### 接口定义

| 操作 | 语义 | 实现行为 | 返回时机 |
|---|---|---|---|
| `memory.record` | 写入新记忆 | 封装转发至 EX 摄入端点，**不直接操作 RMS 图**；内部走完整 EX→SS→RMS 流程 | **同步等待 EX 落库确认后返回**；SS→RMS 后续处理为异步，不在返回路径上 |
| `memory.reinforce` | 强化已有记忆 | 直接修改目标节点 `φ_i`（`s+=0.2`、更新 `n_last_touched`、`reinforce_count+=1`），同步生效，不经过 consolidation worker；**同时异步追加轻量元事件到 EX**（fire-and-forget，见 M7 时间窗合并规则） | 同步（RMS 状态更新完成即返回，不等 EX 元事件确认） |
| `memory.flag_conflict` | 提交纠错 | 封装为携带被纠正事件引用的普通事件，转发至 EX 摄入端点走正常入链；consolidation 阶段据此建立 supersedes 边并对旧节点异步施加 −0.5 | 同 `memory.record`（同步等 EX 确认） |
| `memory.retrieve` | 检索 | M4 四阶段检索流程的外部入口，只读 | 同步返回检索结果 |

### 鉴权设计（强制实现细节）
- IS 为每个写入者身份单独签发凭证。
- JWT claim 结构：`account_id / space_id[] / agent_actor_id / scope`。
- `scope` 取值：`record | reinforce | flag_conflict | retrieve`。
- **`agent_actor_id` 禁止从请求体读取，必须从凭证 claim 解出**——即使请求体传了该字段也要忽略/拒绝，防止同一 space 下多个写入者共享凭证时相互冒充。
- **`debug` scope**：`memory.retrieve` 默认返回结果**不包含**内部 FF 字段（`s_effective`、`n_star_cached`、计数器等）；仅当凭证带 `debug` scope 时才在响应中附带这些字段。**该开关必须绑定 JWT scope，不接受请求参数覆盖**。C 端产品凭证不授予 `debug` scope。

### 验收标准
- [ ] 四个接口的返回时机符合上表（`record`/`flag_conflict` 必须实测验证是等 EX ack 后才返回，不是提交即返回）。
- [ ] 用凭证 A（`agent_actor_id=X`）发起请求，请求体伪造 `agent_actor_id=Y`，系统记录的仍是 X（有测试用例）。
- [ ] 无 `debug` scope 的凭证调用 `memory.retrieve`，响应体中不含 φ_i 内部字段（字段级断言，不是"看起来没有"）。
- [ ] `memory.reinforce` 不触发 consolidation worker（用调用链或指标验证零调用）。
- [ ] 限流/错误码等具体参数留待实施阶段标定，但接口必须预留限流中间件挂载点。

### 明确不做
- 不允许任何接口绕过 EX 直接写 RMS（`reinforce` 除外，且 reinforce 也要异步补 EX 元事件）。
- 不做 `agent_actor_id` 的请求体声明支持。
- 不在 `memory.flag_conflict` 中实现"直接修改旧节点状态"的旧方案（已否决，见 M7）。

---

## 7. M6 — FS 服务（sweep worker）

### 目标
落地 FS 的确定职责边界：只做 sweep 相关的三件事，不做图拓扑推断。

### 职责清单（唯一职责范围，不可扩大）

1. **周期 sweep**：忽视惩罚执行 + 归档判定与执行。
2. **`n_star_cached` 刷新**：节点发生任何 δ 调整时**立即重算**；sweep 过程中**顺带刷新**临近遗忘视界的节点。
3. **固化判定与执行**。

### 忽视惩罚（−0.1）实现规则
- 触发条件：`n_now − n_last_touched ≥ (neglect_count + 1) × N_neglect`。
- 触发动作：`s -= 0.1`，`neglect_count += 1`，**不更新 `n_last_touched`**。
- `N_neglect` 为 agent-level constant，与 λ 同层按域固定，不在运行时改变。
- sweep 按 space 分批执行，节奏只需显著快于 `N_neglect` 对应的事件推进速度，不要求实时。
- **活跃 space 列表来源（v1.2 升级确认定案）**：实现为 `libs/clients` 的 `ControlPlaneStore` 抽象方法（如 `list_spaces()`），**禁止在 FS 服务内直连集群元数据**。M9 映射表落地前的过渡实现：读 EX 集群 schema 元数据，按 `ex_{space_id}` 命名约定（M5 契约 1 已冻结）推导 space 集合——只读控制面元数据，不扫数据、不违反红线 1，也不改冻结契约。已知过渡偏差：`destroying` 中的 space 会被扫入（sweep 幂等，无害），且拿不到 tier（冷热分层节奏在过渡期按统一节奏执行）。M9 落地后切换映射表真身（按 `status=active` + tier 过滤），sweep 代码零改动。
- sweep 幂等性：同一忽视区间至多触发一次惩罚，重复扫描/重跑不产生重复惩罚（用 `neglect_count` 保证）。
- sweep 任务自身必须纳入 Dead Man's Switch 式监控（sweep 停摆 = 忽视惩罚静默失效）。

### 节点生命周期（三种去向，实现为三条独立流程）

| 去向 | 触发条件 | 动作 |
|---|---|---|
| 归档 | `s_effective` 跌破 θ 且按 `n_star_cached` 预测已跨越遗忘视界、再经过宽限期，期间无任何 δ 触发 | 从 JanusGraph 热图移除，归档副本（节点字段+图邻接快照）写入冷存储；EX 原始记录不受影响。**向量处理（v1.2 定案）：归档时同步删除 `rms_vectors` 中对应文档**（不占向量存储、不进锚点候选池），**归档快照必须携带原始 `v_i`**（embedding 不可重放，重算会漂移）；恢复/重建时以快照向量重新写入。**宽限期度量（v1.2 定案）：事件距离**——`n_now ≥ n_star_cached + grace_n` 才归档；期间任何 reinforce/conflict 会把 `n_star_cached` 推过 `n_now`，归档资格自动取消、无需额外状态。**禁用墙钟**：记忆动力学只在 n 域运算（静默 space 的记忆不该随日历衰减）；静默 space 的资源回收归封存层/tier 机制在基础设施层处理，两层不混。冷存储载体（v1.2 定案）：**本 space 自己的 RMS keyspace 内专用表 `archived_nodes`**（直写 CQL、不经 JanusGraph，per-table compaction 可调）；**禁止**共享 keyspace + `space_id` 分区（逻辑分区，销毁退化为 range delete）、禁止入 EX 集群（契约 1 冻结事件两表、source-of-truth 不混派生数据）、禁止 PG。销毁随 RMS keyspace 整体 DROP 自动完成；迁移 snapshot 自动携带（校验项需覆盖该表）；M7 重放重建脚本须一并重建归档表（归档判定可从 EX 确定性重推） |
| 固化 | `reinforce_count` 达阈值且期间无 conflict，或显式调用 | `s` 锁定、跳过衰减计算与 sweep、检索时不再被 θ_effective 过滤。实现标记（v1.2 定案）：置 `consolidated_at` 时间戳 + `n_star_cached` 置 LONG_MAX（保证不被前置粗筛排除）；固化后 ±δ 不改 `s`（计数器照计，计数是事实记录）；固化节点被纠错时 supersedes 边照常建立（检索重定向兜底），固化态 1.0 不解除 |
| 物理删除 | 仅用户主动请求，且**只能是整 space 销毁**（见 M10 注销流程） | 空间内单条事件不支持物理删除 |

### 事件序号 n 的实现要求（M6 依赖，写在此处便于对照）
- 按 `space_id` 独立单调计数。
- 分配点在 EX 摄入路径：事件写入 EX 时获得该空间下一个序号，RMS 节点创建时继承为 `n_created`。
- `n_now` 由摄入路径写穿维护，**Redis 中的 `ex:n` 是权威值（非缓存，无惰性重建路径）**（v1.2 定案）。配套两条硬性运维要求：① Redis 必须开 AOF 持久化（compose 配置 `appendonly yes` + `everysec`）；② 一致性巡检：Redis n vs EX MAX(n) 定期比对，发现 n 回退即告警——防 Redis 数据丢失导致序号重复分配、事件序损坏。
- **只有经验事件推进 n；元事件（如 reinforce 追加事件）不推进 n**——否则会出现"用得越多忘得越快"的语义反转，属于严重 bug。

### 验收标准
- [ ] FS 代码库中不存在任何实体/因果边推断逻辑（该逻辑归 consolidation worker，见 M2/M3）。
- [ ] 忽视惩罚幂等性测试：对同一忽视区间重复触发 sweep，`neglect_count` 只增加一次。
- [ ] 归档/固化/删除三条流程各有独立可触发的测试路径，互不复用同一段代码分支导致误触发。
- [ ] sweep worker 停摆场景下，监控在设定窗口内告警（复用 Dead Man's Switch 机制，见 M13）。
- [ ] 元事件（reinforce 追加）不推进 `n_now`（用计数器前后对比验证）。

### 明确不做
- 不在 FS 中实现实体/因果边推断（consolidation worker 的职责）。
- 不持有 ρ、θ_base 等查询时参数。
- 不支持空间内单条事件物理删除。

---

## 8. M7 — 纠错机制（supersedes）

### 目标
纠错必须实现为"事件"，而不是对旧节点的状态操作。

### 流程（五步，严格按序实现）

1. 调用方经 `memory.flag_conflict` 提交纠错事件，事件体携带被纠正事件/节点的引用。
2. 事件走完整 EX→SS→RMS 入链，在 EX 留下不可变记录（**supersedes 关系必须可从 EX 重放推导**）。
3. consolidation worker 建立 `n_new --supersedes--> n_old` 边，对旧节点异步施加 `−0.5`、`conflict_count += 1`。**同一对节点重复纠错必须幂等**（不重复建边、不重复扣分）。
4. 检索侧（M4）按规则执行：默认重定向至取代者，被取代节点不进候选池（显式追溯历史除外）。
5. 支持链式纠错（A→B→C），沿链取最新有效节点；任何一环的判断变更只是查询策略变更，不需要回写历史。

### 实现红线
- **不设硬失效标志**（tombstone/invalidated 字段禁止出现在 schema 中）。判断"是否返回、如何降权"必须下沉到检索时策略（M4），不能烧进数据层。
- `−0.5` 是软减分，服务于 FF 衰减动力学，与 supersedes 边（记录事实）职责分离，不要合并成一个字段/一次操作。

### reinforce 的 EX 重建性（实现要求，属本模块但触发点在 M5/M6）
- `+0.2` reinforce 保留直连 RMS 的同步旁路（延迟敏感）。
- **必须同时异步追加轻量元事件到 EX**（fire-and-forget，调用方不等确认）。
- 两条约束：
  1. 元事件不计入 `n`（按 M6 事件类型分层规则）。
  2. **同一节点在短时间窗内的多次强化必须合并为一笔追加事件**（附 count），控制 EX 写放大；重建精度降至窗口粒度，此为可接受设计，不是 bug。纠错不做此合并（纠错每笔必须精确）。

### 验收标准
- [ ] schema 检查：节点/边属性中不存在任何"失效标志"字段。
- [ ] 从 EX 重放可以完整重建 RMS 的图结构、δ 调整历史、supersedes 关系（这是 M10 混沌演练"删了重建"场景的前提，必须有可执行的重建脚本与测试）。**节点初始 `s` 来源分两档（v1.2 定案）**：M14 落地前，重建脚本的 `s` 经注入的 `s_resolver` 函数解析、默认占位常数，验收不含 `s` 保真；M14 落地后，`s_resolver` 切换为读 EX `meta_events` 中的 `scoring_result` 元事件，验收升级为含 `s` 的全保真（此时打分维度可重建才真正成立）。
- [ ] 链式纠错 A→B→C 测试：检索默认返回 C；查询"当时认为的是什么"类请求可追溯到 A/B。
- [ ] reinforce 时间窗合并测试：短时间内对同一节点多次 reinforce，EX 侧只产生一条合并事件（带正确 count），RMS 侧每次调用均同步生效。
- [ ] 重复对同一对节点提交 `flag_conflict`，只产生一条 supersedes 边，`conflict_count` 不重复累加。

### 明确不做
- 不实现"−0.5 由调用方直接施加"的旧滥用防护方案（已被事件化纠错消解，纠错滥用统一走摄入层限流/鉴权处理，不是本接口的问题）。

### 实现定案（v1.2，随代码落地入档）
- 纠错处理器 = `lethefield_rms.corrections`（consolidation 纠错职责的最小独立形态，单轮 + CLI；常驻循环/心跳留待 M15 写入链合并）。**幂等 = 单 tx 原子脚本**：tx 内查 supersedes 边，已存在 → 零写入——边即幂等标记，无额外状态表；−0.5 由 `ff.compute_delta` 在 Python 侧预算（Groovy 不算公式纪律不变）。
- reinforce 合并窗口 `REINFORCE_MERGE_WINDOW_MS` 占位 **60s**（§20 待标定）；合并走同主键 UPDATE（count 累加），不动契约 1 表结构。
- 重放重建 = `lethefield_rms.rebuild`：node_key 由 `node_key_of(event_id)` 单点生成（过渡约定，M15 冻结 node_key 生成规则后对齐）；忽视/固化/归档为**理想化 sweep 确定性重推**（假定 sweep 每事件推进点执行——真实节奏不可复现，重建 = 规范历史）；`neglect_due`/`consolidate_due` 判定单点随本模块迁入 `ff`（与 `archive_eligible` 同处，禁止两处副本）；重建边只建双端在热图的（归档节点不落图）；`consolidated_at` 存在性保真、时间戳置执行时刻。

---

## 9. M8 — 记忆空间模型与鉴权（space_id）

### 目标
落地顶层分区键从 `agent_id` 改为 `space_id` 的模型。

### 数据模型要求
- 账号层级：账号（IS）→ N 个记忆空间 → 每个空间内可有多个写入者（Agent/工具）身份。
- **RMS/EX 分区键统一为 `space_id`**（不是 `agent_id`）；`agent_actor_id` 降级为节点属性 `A_i` 的一个字段，不作为分区维度。
- `space_type` 枚举字段（如 `companion` / `project`）：仅作产品/运营维度标注，**不得影响 RMS/FS/SS 核心逻辑**——核心服务代码中不应出现按 `space_type` 分支的业务逻辑。

### 验收标准
- [x] 所有存储层的分区键/routing 键均为 `space_id`，代码库中不存在以 `agent_id` 作为分区键的残留路径（Cassandra/ES 已验证；Pulsar namespace 随 M10 落地时与 C 协作验收）。
- [x] 同一 `space_id` 下不同 `agent_actor_id` 的写入正确共享同一份 EX/RMS，且各自事件可按 `agent_actor_id` 过滤查看来源。
- [x] 核心服务（RMS/FS/SS）代码扫描：无 `space_type` 分支逻辑。

### 明确不做
- 不为 C 端与开发者场景设计不同的底层存储机制——两端共用同一套记忆空间抽象。

### 实现定案（v1.2，随代码落地入档）
- **space_id 字符集正式定义**：`[a-z0-9_]、≤40 字符`（M5 起 ex_n 的 fail-closed 行为转正，已建 space 零迁移）。单点 = `libs/clients/spaces.py` 的 `validate_space_id`（`SPACE_ID_MAX_LEN`），四种存储命名共用：EX keyspace（`ex_n.keyspace_name` 委托）、RMS 图名（= space_id）、ES routing、Pulsar namespace（M10）。fail-closed 语义不变：不合法直接拒绝，不静默改写（防两 space 映射同一存储名造成跨 space 混流）。
- **API 入口防线**：service 四操作（record/flag_conflict/reinforce/retrieve）在 `require_space` 后统一 `_require_valid_space`，非法 space_id 一律 400 `bad_request`、零副作用（不触图名/keyspace 命名路径）。
- **`space_type` 落点**：`SpaceType` 枚举（`companion`/`project`，spaces.py）+ `SpaceMapping.space_type` 可选注解字段（默认 None，向后兼容 M0 冻结接口；M9 映射表沿用）。仅产品/运营标注，核心服务零引用。
- **验收落点**：多写入者共享 EX/RMS + 来源过滤 + 非法 space_id 零副作用 = `tests/integration/test_m8_space_model.py`；「无 agent_id 分区键残留」「核心服务无 space_type 引用」= `scripts/check_space_model.py` 静态巡检（不起栈，已接 ci.sh；space_type 仅允许出现于 libs/clients 与巡检脚本自身）。

---

## 10. M9 — Cell 架构 + 租户调度器

### 目标
落地容量管理的控制面，1 Cell = 1 Cassandra 集群 + 1 ES 集群 + 逻辑关联计算池分组，space 归属唯一 Cell。

### 三层结构

```
控制面：租户调度器（无状态服务 + 元数据存储）—— space→Cell 映射 / 开通 / 迁移 / 注销 / 水位
计算面：JanusGraph 共享池 + LRU 图实例缓存，缓存键为 (space_id, cell_id)
存储面：Cell 池（对等 Cell 水平集合，互相不感知、无跨 Cell 通信）
```

### 元数据模型
- `space_id → {cell_id, ex_cluster_id, pulsar_cluster_id, status(active|migrating|destroying), tier}`
- `cell_id → {endpoints, capacity, watermark_state}`
- 存储于独立小 Cassandra 集群（或 Cell-1 专用 keyspace），接口抽象为 `ControlPlaneStore`（预留服务商替换扩展点，2.0 用）。
- **强制要求**：映射表必须有备份与导出机制。映射表丢失 = 全部 space 失联（数据还在，入口丢失），这是 1.0 验收硬指标，不是可选项。

### 开通流程

**统一以 M10「三存储生命周期流水线」为最终版本**（顺序：EX keyspace → Pulsar namespace → RMS keyspace + ES index → 注册三向映射，失败回滚）。本节只保留 Cell 特有步骤：

1. 选水位最低的 `open` Cell。
2. 按 M10 顺序建存储；其中 RMS 侧为 `createConfiguration` 建图（预期 ~0.9s，见 spike 数据）+ ES 建 index（冷 space 直接 1 主 0 副本）。
3. 注册映射：**先存储后注册，失败回滚**。
4. `hot`/`premium` tier 触发计算池预 open，消化冷开 ~3.6s 延迟。

### 迁移流程（跨 Cell 搬迁）
1. 标记 `migrating`（读正常、写短暂只读，只读窗口目标 <1 分钟）。
2. Cassandra snapshot + sstableloader；ES snapshot/restore。
3. 校验。
4. 切映射恢复写。
5. 源侧宽限期（如 7 天）后按注销流程销毁。
6. 触发场景仅限三类：Cell 退役、跨服务商迁移（退化为文件级导出/导入）、人工拍板的再平衡。**不做自动触发的再平衡**。

### 注销流程

**统一以 M10「三存储生命周期流水线」为最终版本**（先派生物后本体 + 训练管线销毁广播 + 全链路校验无残留）。本节只强调 Cell 特有的硬性顺序：**计算池驱逐图实例必须先于任何 `DROP KEYSPACE`**（红线 5，规避在线 DROP 导致的 `Unknown CF` 污染）。

### 水位制调度（三档状态，阈值为初值待标定）
- `open`：全维（keyspace 数/ES 分片数/磁盘/堆压力）< 70%，接受分配。
- `filling`：任一维 70~90%，停止新分配、筹备新 Cell。
- `closed`：任一维 > 90%，只出不进、告警。

**明确不做（设计已否决，不要自行实现）**：
- 不做自动再平衡（数据迁移是最重最贵操作，自动触发=容量问题放大成迁移风暴）。
- 不做动态打分装箱（热点问题用 tier 升降解决，不搬迁）。

### 调度器高可用要求
- 无状态多副本部署。
- **存量读写不经过调度器**——计算池持映射缓存直连 Cell，调度器宕机只影响"不能开通/迁移/注销"，不影响已有 space 的读写（控制面故障不扩散到数据面，这是硬性验收项）。

### 验收标准
- [ ] 映射表备份/导出脚本存在且可用于灾难恢复演练。
- [ ] 开通流程顺序正确（以 M10"EX → Pulsar → RMS → 注册"为准）：存储步骤失败时注册不执行，且已完成的存储资源被回滚清理。
- [ ] 注销流程严格按"先驱逐计算实例、再删存储"执行；模拟在线 DROP keyspace 会触发的 `Unknown CF` 问题在本流程下不出现（因为顺序已规避）。
- [ ] 调度器全部实例下线情况下，已有 space 的 `memory.record`/`retrieve` 等接口仍可正常工作（用故障演练验证）。
- [ ] 水位状态转换（open→filling→closed）在模拟负载下正确触发，且触发后不再向该 Cell 分配新 space。

### 明确不做
- 不实现自动再平衡逻辑。
- 不实现动态打分装箱调度算法。
- 不在调度器 API 中暴露 Cell 内部高可用细节（属 2.0 课题范围）。

---

## 11. M10 — EX 存储与 Pulsar 归属 + 三存储生命周期流水线

### 目标
落地"持久资产按资产归属分片，传输管道按吞吐供给池化"的统一原则。

### EX 事件存储实现要求
- 每 space 一个独立 EX keyspace，存放于**与 Cell 池平行**的 EX 集群池。
- 由同一租户调度器统一登记（映射表扩展字段 `ex_cluster_id`）与水位调度（复用 M9 §17.3 机制，不新增新调度逻辑）。
- **禁止并入 Cell 的 Cassandra**（集群故障会同时切断数据与重建源，违反可重建性前提）。
- **禁止退回逻辑分区方案**（单一大集群 + `space_id` 分区键）——整 space 销毁在该方案下会退化为 range delete（tombstone 风暴、不可校验无残留），不满足结构性保证要求。
- 成本缓解措施（必须落地，不是可选优化）：EX 使用高密度低规格机型；**封存层优先封存 EX**（冷 space 的 EX keyspace 是第一批封存对象）。

### Pulsar 实现要求
- 每 space 一个 namespace（不变）。
- **生产侧（v1.2 定案）**：EX 经验事件由 **API 摄入路径**在 `append_experience` 落库确认后发布到 `lethefield/{space_id}` topic——有限重试 + 退避，**发布失败不阻塞同步返回**（EX 已是 SoT，同步返回语义不变），最终失败触发 page 级告警 + 指标。**M5"API 不依赖 Pulsar"结构性红线同步修订为：同步返回路径不依赖 Pulsar**（发布在 ack 之后、失败可容忍），producer 依赖显式登记；M5 集成测试断言按修订后口径更新。
- **消费侧自愈（v1.2 定案，免新组件）**：n 是 per-space 连续序号——M14/M15 消费侧校验 n 连续性，发现缺口即 page 级告警并触发补偿（按 n 区间从 EX 重放补发）；消费方幂等（M15 `ref_ex` 唯一判定已定案），重放/重试产生的重复安全。不新立轮询 relay 组件。
- **全局集群池，不随 Cell 分片**（禁止实现 per-Cell Pulsar）。
- 池按总吞吐/总 backlog 扩容，与 space 总数弱相关。
- 演进路径（触及瓶颈后才做，1.0 默认全局单池）：粗粒度分片池，一个 Pulsar 集群服务 K 个 Cell，K 待实测标定，届时由调度器登记映射。

### 三存储生命周期流水线（覆盖 M9 的开通/迁移/注销，本节为最终版本）

**开通顺序**：EX keyspace → Pulsar namespace → RMS keyspace + ES index → 注册三向映射。

**迁移**：EX 与 RMS 各自走快照/导入流水线（可并行）；Pulsar 侧在途事件处理：暂停摄入（生产者明确失败并重试缓冲）→ consumer 排空源 backlog → 切映射 → 目标侧恢复 → 源 namespace 宽限期清理。只读窗口目标 <1 分钟（由最慢者决定，通常是 EX）。

**注销**（严格按序，五步 + 广播）：
1. 标记 `destroying`。
2. 驱逐计算实例（红线 5）。
3. **先派生物后本体**：`DROP RMS keyspace` + `DELETE index` → 删 Pulsar namespace → 最后 `DROP EX keyspace`（任何中途失败都不至于"图没了、源也没了"）。
4. **向训练管线广播销毁指令**（对接 M11：存量样本内容清除、注册表项删除）。接口形态（v1.2 定案）：独立训练 tenant 下的**持久化控制 topic**，destroy 发布结构化销毁指令消息；M10 附带最小接收 consumer 落接收记录证明链路真实可达，M11 落地后以真实加工 worker 消费同一 topic（topic 不变）。三条硬性约束：① 生产者**必须等 broker ack**，最终失败则本步骤标记失败、告警并留痕，**禁止静默进入第 5 步**（销毁指令是义务级，不是 fire-and-forget）；② 控制 topic 的 consumer backlog 纳入监控（consumer 停摆 = 销毁指令静默积压 = 合规失效）；③ 指令消息 schema 单点定义（与契约 1 同处管理），M10 冻结最小字段集（`space_ref` 哈希 / 指令类型 / 发起者 / 时间戳 / 注销工单引用），M11 可向后兼容扩展。
5. 清缓存与映射，全链路校验无残留。

### Dead Man's Switch（对接 M13，此处列出实现要求）
- EX 摄入层必须配置独立心跳/dead man's switch 告警：若某活跃 space 在设定时间窗口内完全没有新事件写入，主动触发告警。**这是硬性要求，不是可选项**。
- **落地形态（v1.2 定案）：双路，监控信号与数据面物理分离**：
  1. **管道活性探针**：独立巡检进程向**集群级专用 probe topic**（监控 tenant，不属于任何 space）发探测消息并自消费 ack，验证 broker → BookKeeper → consumer ack 端到端活性。**禁止向 space 的 namespace 发探针**——探测消息会进入 EX 摄入/SS 真实消费链，污染数据面。探针告警为 page 级（管道死了）。
  2. **space 写入新鲜度**：「活跃」的客观定义——过去 W 窗口内有 ≥1 次成功摄入的 space，或 tier=`hot`/`premium` 的 space 全集；自上次成功摄入起超过超窗阈值无新事件即告警（数据源：摄入路径维护的最近写入时间，经 `ControlPlaneStore` 接口读取，禁直连存储）。新鲜度告警为 observation 级（进留痕 + 看板，不呼叫）——它区分不了"用户合法闲置"与"客户端静默断裂"，语义就是弱信号，按弱信号处理。W 与超窗阈值待标定（§20），开发期占位。
- 两路告警事件走 `libs/logschema` 结构化日志 + `pulsar_backlog_events` 指标（M12 载体）。

### 验收标准
- [ ] EX 集群与 Cell 的 Cassandra 集群物理独立，可通过连接串/集群 ID 区分。
- [ ] 开通流程按"EX → Pulsar → RMS"顺序执行，任一步失败触发已完成步骤的回滚。
- [ ] 注销流程按"先派生物后本体"顺序执行，且第 3 步之前已完成计算实例驱逐（用于规避在线 DROP keyspace 问题）。
- [ ] 注销流程第 4 步必须实际调用训练管线的销毁指令接口（不是留空占位）。
- [ ] Dead Man's Switch：模拟某 space 停止写入超过设定窗口，触发告警（不依赖"系统看起来在正常运行"这一假设，必须是主动探测）。
- [ ] 迁移演练分两档（v1.2 定案）：① 本地档——RMS 侧真跨 Cell（compose `cell2` profile 按需起第二 Cell，默认不进 CI），EX 侧同集群 keyspace 快照/复制演练（snapshot → load → 校验 → 切映射 → 宽限清理全步骤真实执行）；端到端跑通并实测只读窗口时长、写入演练记录。② 准出档——阶段 1 准出前，在临时扩容环境（colima 调大内存或另一台机器）补跑一次**含 EX 跨集群 sstableloader 流式传输**的完整迁移演练，实测数据入演练记录。已知缺口登记：EX 跨集群传输路径本地档不覆盖，准出档补齐。

### 明确不做
- 不实现 per-Cell 独立 Pulsar 集群。
- 不把 EX 并入 Cell 的 Cassandra 集群。
- 不采用逻辑分区（单一大集群+分区键）方案存储 EX。

---

## 12. M11 — 训练数据管线（1.0 最小实现）

### 目标
落地 §12.4 定义的最小集：让"高价值判断时刻"以结构化、可撤回的方式沉淀，管线本身不查业务库。

### 四入料口

| 入料口 | 内容 | 优先级 |
|---|---|---|
| ① 决策留痕对比数据 | §11.3 Agent 建议 vs 人类最终决策 | 价值密度最高，1.0 必做 |
| ② 故障与混沌工程案例 | §11.4 演练产出 | 1.0 必做 |
| ③ 检索质量与 FF 标定明细 | §19.2 space 粒度日志事件，复用 M12 §19.6 schema | 1.0 必做 |
| ④ 用户记忆内容副本 | EX 派生加工品（纠错链、supersedes 前后对、高质量片段） | **仅限 §12.3 授权覆盖的 space**；只读 EX、产出独立副本、永不回写 |

### 高价值时刻判定规则（1.0 实现 R1–R3，R4–R6 可延后）

| 规则 | 内容 | 1.0 是否实现 |
|---|---|---|
| R1 | 人类否决/修改 Agent 建议 | ✅ |
| R2 | §11.2 升级四类事件 | ✅ |
| R3 | 召回内容被纠错 | ✅ |
| R4 | supersedes 链形成 | 可延后 |
| R5 | 演练与真实事故复盘 | 可延后 |
| R6 | 人类未否决但事后证明错误（种子期后启用） | 可延后 |

**刻意不做全量沉淀**——规则未命中的常规流量只进运维日志滚动清理，不进训练管线。这是设计原则，不是性能优化，不要为了"数据多总是好的"擅自扩大采集范围。

**R3 检测机制（v1.2 定案，关联式实现）**：
1. retrieve 后发射召回明细日志事件（logschema，进运维日志管线——③ 入料口的既定数据源）。字段最小化：`space_ref` 不透明哈希 + 召回 `node_key` 列表 + θ 统计 + retrieve 时间戳 + query 类别枚举；**不含** query 原文与内容摘要（基数纪律）。
2. M11 生产侧过滤器读日志管线、查授权注册表，**未授权 space 的明细在入训练 topic 前拦截**（③④ 类既定拦截点不变）；授权 space 的明细**过境**训练 topic（短 retention，不沉淀）。
3. 加工 worker 将召回明细与 ④ 路纠错对事件按 `node_key` + 时间窗 `W_r3` 关联，**命中才产 R3 样本**；未命中明细随 retention 滚动清除。
4. 语义边界：R3 是检索质量信号，**未经召回的纠错不计入 R3**（属别的价值类别，归 R4/R6 再议）；"纠错即样本"的简化方案会污染标定闭环，已否决。过境 ≠ 沉淀，与"刻意不做全量沉淀"不冲突。

### 管线架构
```
数据源 → 训练 topic（Pulsar 独立 tenant/namespace，与业务流 retention/配额隔离）
       → 加工 worker（格式封装、授权校验、批次落盘；只从 topic/对象存储取数，不查业务库——红线 1）
       → 对象存储分层：
            热（近 90 天，待标注+已标注，JSONL）
            温（approved 样本主集，长期，JSONL）
            冷（年度快照，深度归档，Parquet）— 可延后
            废弃（rejected 与被撤回骨架，1 年后清除）
```

### 样本 schema（统一格式，1.0 必须实现）
```
{
  sample_id, source, rule, space_ref,   // space_ref 为不透明哈希，不暴露 space_id 明文，支持成组定位
  problem, diagnosis, decision, outcome,
  auth_scope, review
}
```
- 真人决策岗处置时顺带标注（决策留痕表单即标注界面，**不新增标注工种**）。
- 模型辅助预标注 + 抽检：可延后（种子期后再启用）。
- 未经 `approved` 不进正式训练集。

### 授权撤回与删除权联动（1.0 必须完整实现，不是可延后项——"义务不是功能"）

1. **授权注册表**：加工 worker 每批次必查；**未授权 space 的第 ③④ 类数据在入 topic 前被拦截拒绝**（不是"进来再删"）。
2. **撤回授权**：停止新增采样 + 存量样本按 `space_ref` 哈希成组定位 → 内容字段清除、脱敏判断骨架可留。
3. **整 space 销毁**：消费 M10 控制 topic（训练 tenant，见 M10 注销第 4 步定案）中的销毁指令 → 同上等效处置 + 注册表项删除；处置动作本身进决策留痕。
4. **可定位性实现要求**：哈希 + 样本索引清单，使定位是 **O(清单) 操作**，不是全量扫描。**必须 1.0 内建，不接受"后续再补"的实现**——这是可撤回性的前提，后补等于重建整套定位机制。

### 验收标准
- [x] 未授权 space 产生的第 ③④ 类数据，加工 worker 在入 topic 前即拒绝（有测试用例：授权状态切换前后对比行为）。
- [x] 授权撤回后，对应 `space_ref` 的存量样本可在 O(清单) 时间内定位并完成内容清除（不是遍历全部样本）。
- [x] M10 注销流程触发的销毁广播，训练管线侧有实际接收与处置记录（决策留痕可查）。
- [x] R1–R3 规则各有可触发的测试场景，命中后产生符合 schema 的样本；未命中规则的常规流量不产生训练样本。
- [x] 加工 worker 代码审查：不存在任何直接查询业务库（RMS/EX 实时库）的调用路径，只读 topic/对象存储。

### 明确不做
- 不做全量流量沉淀。
- 不在 1.0 实现 R4–R6、模型预标注、冷层 Parquet 自动化（可延后项，不是不做，是不在本轮验收范围）。
- 不在 2.0 服务商场景授权结构确定前，扩大训练数据来源范围。

### 实现定案（v1.2，随代码落地入档）
- **feed 信封与 topic 单点** = `libs/clients/training_feed.py`：`persistent://lethefield-training/feeds/raw`（单 topic + 信封 kind 四值，1.0 不按源拆 topic；短 retention 7 天占位，**过境 ≠ 沉淀**）；R1/R2 判定纯函数 `decision_rules` 单点，提交路径与 worker 共用。销毁订阅名 `DESTROY_SUBSCRIPTION` 单点上移至 `training_control.py`（M10 sink 与 worker 共用继承积压）。
- **授权注册表归属**：独立 Postgres 表（M0 已建，§12.4 待决项 1 按现状收口）；store 下沉 `libs/clients/auth_registry.py`（API 闸门/worker/ex-feed 多处共用，共享代码只允许 libs/），新增 `delete`（销毁处置删注册表项）；ops/auth_registry 仅剩薄 CLI re-export。
- **① 口**：decision_log 补齐 §11.3 三列（M0 任务 5 定案），幂等迁移 `deploy/postgres/migrations/001_decision_log_m11.sql`；R1/R2 命中才发布（best-effort，失败只留痕不阻塞提交；① 类无用户内容，不过授权闸门）。
- **③ 口过渡形态**：召回明细 LogEvent 恒发（运维日志管线，M12 收口）；授权闸门 1.0 在 API retrieve 进程内执行（同一拦截点语义：入 topic 前、查注册表、CALIBRATION scope），M12 日志管线上线后过滤器可改为读管线。明细字段最小化：无 query 原文与内容摘要；θ 统计数据源 = `RetrievalResult.stats`（anchors/pool/returned，四阶段签名不动）。
- **④ 口**：`lethefield_training.ex_feed` 只读 EX 派生纠错对（旧内容按 `ref_conflict` 反查 EX 事件，不触 RMS；缺一半的对不喂）；CONTENT_COPY 未授权直接拒发；state 文件幂等。1.0 仅纠错对（供 R3），纠错链/高质量片段独立样本随 R4 延后。
- **worker**：双订阅单进程轮询（feed `training-sample-worker` + control `training-destroy-sink`）；worker 侧授权复查为第二道防线；热层 = 本地目录（无对象存储，1.0 工程从简）`hot/samples-日期.jsonl` + `index/{space_ref}.jsonl` 清单索引；`scrub` 只读目标清单按单重写（O(清单)），内容字段清空、骨架保留；召回窗落 `recall_window.jsonl` 重启可重建；`W_r3` 默认 24h 占位（§20 待标定，env 可配）；worker 侧模块禁 import gremlin/cassandra/ex_n（红线 1，静态测试强制）。
- **样本规则标签**：② 口人工提交即触发，rule 标 R5（无自动检测，R5 自动规则仍属延后）；R3 语义边界——未经召回的纠错不计入 R3（归 R4/R6 再议）。

---

## 13. M12 — 可观测性埋点（开发期最小集）

### 目标
落地 §19 定义的三线分类指标，开发期最小集为**服务完成的验收条件之一**（不是锦上添花）。

### 设计原则（实现约束）
1. 每个指标必须挂在明确消费方上（待标定参数/告警/决策留痕），不做"反虚荣"指标。
2. 三线分类：告警线（实时）、标定线（离线分析）、留痕线（审计与校准）。
3. **基数纪律**：`space_id` 禁止作为聚合指标标签；space 粒度明细走日志管线（M1 部署的运维日志 ES，见 M1 任务 4），聚合指标只保留低基数标签（`service`/`cell_id`/`tier`/`result` 类枚举）。**聚合永远走旁路，不扫存储**。
4. 指标按 OpenMetrics 语义定义（counter/gauge/histogram），推荐 Prometheus + Grafana。

### 开发期落地清单（= 服务完成验收条件，缺一不可）

**1. 系统指标（告警线，全部落地）**：

| 指标 | 说明 |
|---|---|
| `graph_open_duration_seconds{type=cold/warm}` | 基线：cold p50≈3.6s / warm≈0.48s，用于漂移告警。**口径（v1.2 定案）**：客户端近似——在显式 open 点（provision 预热 / migrate / rebuild / schema 初始化）埋 histogram，cold/warm 按图名是否已存在推断；运行期逐 traversal 的服务端隐式 open 不进统计（开发期近似口径，诚实标注） |
| `graph_lru_cache_hit_ratio` | LRU 缓存与预热标定。**口径（v1.2 定案）**：离线推导代理指标，非服务端真实命中率——从 space 粒度检索延迟日志事件（召回明细，M12 已有）离线计算"闲置间隔后首请求且 Stage 耗时异常高"的请求占比，作为缓存失效频率代理，回答"缓存是否够大、预热是否有效"。**否决 JMX sidecar 方案**（JG 是否暴露图实例缓存指标未验证 + 新组件过重）；2.0 生产规模下缓存容量成为真实问题时再议服务端真实口径 |
| `retrieve_stage_duration_seconds{stage=knn/subgraph/ff_filter}` | 检索各阶段耗时 |
| `record_confirm_duration_seconds` | `memory.record` 确认延迟预算 |
| `pulsar_backlog_events{namespace_class}` | **Dead Man's Switch 载体**，见 M10 |
| `ex_write_duration_seconds` | EX 写路径耗时 |
| `ex_last_write_age_seconds`（聚合：max / p95） | 原 `n_now_lag_seconds` 的口径修正（v1.2 定案）：原语义"缓存 n 滞后于 EX"已被实现消灭（摄入路径写穿维护、Redis n 即权威值），现口径 = **space 距最近成功写入的时长**（数据源 `ex:last_write:{space}`，M10 已落地）。由 DMS 巡检进程顺带发射聚合 gauge（max / p95，**禁 space_id 标签**）；与 DMS 新鲜度告警同源同事、各走各线（告警线管呼叫，指标线管趋势） |
| `fs_sweep_lag_seconds` / `fs_sweep_processed_total` | FS sweep 健康度 |
| `cell_watermark{cell_id,dimension}` | M9 水位直接输入 |
| `space_storage_bytes{tier}` | 成本验证，需合并 EX 口径 |

**2. FF 骨架指标（标定线，最小三项）**：`ff_theta_filter_ratio`、`ff_recalled_then_touched_rate`、`ff_delta_applied_total{type}`

**3. 留痕线两项**：`agent_suggestion_total{outcome=accepted/modified/rejected}`、`escalation_total{reason}`

**4. 日志事件 schema（必须实现）**：space 粒度明细结构定义——这是离线标定指标的原料，也是 M11 入料口 ③ 的接口。

**可延后（不在开发期最小集内）**：§19.2 其余离线 gauge（`ff_s_effective_dist`、`ff_conflict_then_superseded_rate`、`ff_neglect_then_revived_rate`、`retrieve_empty_result_total`、`retrieve_corrected_total`、`retrieve_anchor_kept_ratio`、`retrieve_kept_then_used_rate` 等，种子期前补离线聚合任务）；`cell_watermark` 随 M9 落地时同行。

### 埋点实现规范
- 命名规则：`lethefield_<域>_<名称>_<单位>`（Prometheus 标准单位）。
- 请求路径同步打点只写聚合指标（进程内 O(1)）；space 明细写结构化日志事件（异步批量进运维日志 ES 集群）。
- **标签白名单**：`service`/`instance`/`cell_id`/`tier`/枚举类。**标签黑名单**：`space_id`、`node_key`、内容摘要（防基数爆炸 + 守红线 1）。
- 与训练管线边界：运维指标/日志与含用户内容的训练副本，**在埋点代码层面物理分开**（不是同一份日志两处消费）。

### 验收标准
- [x] 上述"1+2+3+4"四类指标/schema 全部实现且可在 Prometheus/Grafana 查询到。
- [x] 代码审查：任何聚合指标的标签中不出现 `space_id` 或 `node_key`。
- [x] 聚合指标计算路径不扫描存储（验证方式：聚合任务的数据源只能是日志管线，不能是 RMS/EX 直连查询）。
- [x] 埋点代码与训练管线数据源代码物理分属不同模块/文件，无共用埋点函数。
- [x] M11 ③ 入料口收口为定案形态（v1.2 定案）：API retrieve 只发 LogEvent 进运维日志 ES；过滤器轮询日志管线 → 查授权注册表 → 转发训练 topic（at-least-once + checkpoint，召回明细事件带唯一 ID、worker 侧按 ID 去重）；**API 进程内授权闸门与训练 topic 发布代码整体删除**（不是注释掉），M11 相关集成测试改为定案形态。

### 明确不做
- 不在开发期实现"可延后"清单中的离线 gauge（留到种子期前）。
- 不做 Prometheus HA（2.0 再议）。

### 实现定案（v1.2，随代码落地入档）
- **暴露口**：API 走自身应用端口 `/metrics`（运维通道不挂业务 scope，1.0 内网暴露口径）；常驻 worker（fs 9101 / training 9102 / ingest_dms 9103 / metrics_exporter 9104）经 `lethefield_metrics.start_metrics_server`（prometheus_client 薄封装，env `LETHEFIELD_METRICS_PORT` 可覆盖，0 = 不起；--once 短命形态不起）。
- **日志管线**：`libs/logschema/es_sink.py` 是唯一写入端——`emit(event, sync=)` stderr 恒留底 + 已 configure 进程异步批量 bulk 进 es-ops（index `lethefield-logs-YYYY.MM.DD`，动态 mapping 开发期够用）；一次性 CLI 用 `sync=True`（惰性建 shipper 同步直写防退出丢尾部）；ES 不可达 fail-open 不阻塞业务。既有 13 处 print 发射点全部收编。
- **离线聚合** = `ops/metrics_exporter`（循环 + --once + :9104）：留痕线/δ counter 与 graph_open histogram 从 es-ops 日志流折叠（游标只在进程内，重启全量重建——开发期日志量有界）；`ff_recalled_then_touched_rate` / `graph_lru_cache_hit_ratio`（代理口径）按窗全量重算；`cell_watermark_ratio`（命名规则强制 _ratio 后缀）读映射表；`space_storage_bytes{tier}` = Cassandra `system.size_estimates`（cell+EX 两集群）+ rms_vectors 总字节按 `/_count?routing=` 文档数比例分摊（共享索引无 per-space 字节的近似口径）。**δ/留痕计数统一走日志流聚合通道**——一次性 CLI（corrections 等）的进程内 counter 不可 scrape，这是它们的指标到达 Prometheus 的唯一通路。
- **③ 收口**：`lethefield_training.recall_filter`（recall-filter 子命令）轮询 es-ops → CALIBRATION 闸门 → 转发 feeds/raw；召回明细带 event_id（API 侧 uuid4）+ stage_ms；worker `RecallWindow.mark_seen` 按 ID 去重（缺 event_id fail-closed）。
- **Prometheus + Grafana** 进 compose（9090/3000；scrape 走 host.docker.internal——服务进程在宿主机 uv run 形态；Grafana provisioning datasource + 三线六面板 dashboard）。

---

## 14. M13 — 多租户工程红线落地

### 目标
把 §11.5 六条红线转化为可自动化检查的工程约束，不是文档层面的口头要求。

| 红线 | 要求 | 实现方式 |
|---|---|---|
| 红线 1 | 禁止全局广播/跨 space 全集群扫描 | 运维后台、批处理、数据分析任务入口**必须**绑定显式 space 列表参数（argparse 含 `--space(s)`，静态扫描强制）；代码审查/静态检查禁止出现无 space 过滤的全表扫描调用。**常驻 worker 合规定义（v1.2 定案）**：红线禁止的是无 space 纪律的数据面扫描；经 `ControlPlaneStore.list_spaces()` 枚举、逐 space 独立处理的常驻 worker（FS sweep / DMS / exporter 等）**本身即合规**，但须满足豁免三要件并机器可读登记（如 `@redline1_exempt(worker=..., reason=...)`，静态扫描器只认登记不认注释）：① 枚举走 ControlPlaneStore（禁直连存储元数据）；② 逐 space 独立处理单元（故障隔离、无跨 space 查询）；③ 批间节流（登记时声明节奏，防豁免 worker 变成伪装的全集群轰炸）。新增豁免必须显式登记，可审查 |
| 红线 2 | 单 space 资源配额 | 顶点/边数量上限、ES 侧单 space 存储与向量条数上限、单次图遍历最大跳数与返回节点数上限，均需在配置中显式定义并在写入/查询路径强制校验。**落地形态（v1.2 定案）**：`QuotaConfig`（全局默认 + 按 tier 可选覆盖，1.0 不做 per-space 定制）；写入侧顶点/边/向量条数硬校验——落在现有 `writer.py` 与向量写入路径（不等 M15 接入），图 count 带短 TTL 缓存摊销（**近似执行、超发有界**，配置注释写明）、ES count 走 O(1) count API；查询侧复用 `RetrieveConfig` 的 max_depth/beam_width/token_budget 并**补返回节点数硬上限**；内部抛 `QuotaExceeded`，API 层映射既有 429 `rate_limited`（不新增契约错误码，`message` 区分 quota 字样）；ES 存储字节不上硬校验，走 `space_storage_bytes` 指标监控 |
| 红线 3 | 冷热分层 | 热 space 保障资源；冷 space 降低 sweep 频率；Cell 架构下每 space 独立 mixed index，冷 space 可做索引级降配/冻结（设计文档 §17.2）；共享向量索引（`rms_vectors`，custom routing）无法按 space 冻结，其冷热控制靠 M6 归档机制。**Redis 逐出项豁免（v1.2 定案）**：现存 Redis 键均为小键（`ex:n` 权威计数、`ex:last_write`、sweep 心跳），无大对象，逐出无内存收益且 `ex:n` 逐出会破坏 n 分配——不设逐出策略、不配 maxmemory-policy 逐出（防未来误伤权威键）；未来出现真实大对象缓存层时再议冷热逐出 |
| 红线 4 | `ids.authority.wait-time` 保持默认 | 见 §1 强制配置项，需巡检脚本 |
| 红线 5 | 禁止在线 DROP keyspace | 所有涉及 keyspace 删除的流程（M9/M10 注销）必须先驱逐计算实例 |
| 红线 6 | 节点时钟同步硬性前提 | NTP 硬化 + 偏移监控，见 §1 |

### 验收标准
- [ ] 静态扫描/代码审查工具能拦截"无 space_id 过滤的批量查询"提交。
- [ ] 单 space 配额在压测下触发限流/拒绝（不是仅存在配置项而未强制执行）。
- [ ] 冷 space 的 sweep 频率确实低于热 space（对比测试）；Redis 逐出项按豁免论证处理（v1.2 定案：现存键均为小键无内存收益、不设逐出策略，论证写入汇总核验记录）。
- [ ] Redis 持久化与 n 一致性巡检落地（v1.2 定案，配合 `ex:n` 权威值形态）：compose 配 AOF（`appendonly yes` + `everysec`）；巡检脚本比对 Redis n 与 EX MAX(n)，回退即告警。
- [ ] 红线 4/5/6 均已在 M1/M9/M10 中有对应验收项，此处做汇总核验。

---

## 15. M14 — SS 显著性打分服务

### 目标
落地六维度显著性评分（ER/E/I/G/N/C）的生产路径：消费 EX 事件流，产出结构化打分，供写入链（M15）消费。SS 在 v1.0 中只有零散要求、无责任模块，本模块补齐。

### 实现要求
- **形态**：Pulsar consumer，消费 EX 事件流（每 space namespace），逐事件打分。
- **打分方式**：LLM prompt 六维打分（1.0 唯一实现路径）。**这是全项目最大的效果风险项，必须在开发早期用真实样例验证打分稳定性**（分布是否可用、维度间是否塌缩、成本是否可承受），验证结论进决策留痕。
- **客户端形态（v1.2 定案）**：OpenAI 兼容 HTTP 直连（复用 httpx，不引入 provider SDK）；`base_url`/`api_key`/`model` 全走 env，provider 中立（通义/DeepSeek/OpenAI/Moonshot 兼容端点均可配）；key 只走 env、不进仓库不进日志；六维输出的解析失败/字段缺失走本模块降级规则（禁隐式兜底）；打分调用量与估算成本打标定线指标（SS 是 1.0 唯一按事件付费的外部依赖，成本曲线是标定输入）。
- **输出**：结构化打分结果（六维分值 + 事件引用 + 打分模型版本），写回 Pulsar 下游 topic，供 M15 消费。
- **打分结果回写 EX（v1.2 定案，保真重建前提）**：打分产出后，以 `scoring_result` 元事件追加写入该 space 的 EX `meta_events` 表（六维原始值 + 合成 `s` + 模型版本 + 事件引用；**不推进 n**，与 reinforce 元事件同规约）。理由：LLM 打分不可重放（模型版本/采样漂移），若不持久化到 EX，RMS 销毁重建时原始 `s` 永久丢失，"EX 重放完整重建 RMS"在打分维度不成立。元事件表是契约 1 已有的 envelope（新增事件类型、不改摄入路径）。**契约 1 首次演进（v1.2 修订记录第 6 条配套修正）**：`meta_events` 加可空 `details` 列（text/JSON，按 meta_type 分型、schema 单点定义；reinforce 等既有类型置空、行为不变）——原"不改表结构"表述以本演进为准。契约 1 演进规则自此明确：**只允许"可空加列"式兼容演进；改语义/改键/改摄入路径视同契约修改，走完整升级确认**。
- **权重不硬编码**：六维合成 `s` 初值的权重为待标定参数（设计文档明确禁止硬编码），以配置项占位，打分原始六维值与合成 `s` 分开存储，标定后只需调整合成逻辑、无需重打分。
- **可靠性**：打分失败进死信队列 + 重试 + 告警，**禁止静默丢弃**（丢打分 = 事件进不了 RMS，属于数据丢失）。
- **降级规则（v1.2 定案，按坏的程度分级）**：① JSON 可解析、仅缺 **1 个**维度 → 该维置中性值（默认 0.5，可配）+ `degraded` 标记 + 缺失维清单，进下游并随 `scoring_result` 回写 EX（未来模型升级/权重标定后可识别、可重打分；1.0 只保证可识别），指标 `lethefield_ss_score_degraded_total{dim}` 计数；② JSON 不可解析或缺 **≥2 个**维度 → 视为打分失败，走重试 → 耗尽进 DLQ（缺两维以上合成 `s` 无意义，不是降级是失败）。配置开关 `SS_DEGRADE_POLICY=neutral_mark|retry`，默认 `neutral_mark`（种子期降级面过大可切 strict）；不允许代码里隐式兜底。
- **打点**：打分分布（六维分值直方图、合成 s 分布）进标定线指标，供设计文档 §19.2 标定使用。

### 验收标准
- [ ] 事件从 EX 写入到打分结果产出，端到端链路可追踪；打分结果含六维原始值与合成 `s`，权重来自配置而非代码常量。
- [ ] 打分失败场景（LLM 超时/返回格式异常）进入死信并触发告警，重试恢复后不丢单、不重单。
- [ ] 打分稳定性验证报告存在（真实样例集上的分布统计），结论进决策留痕。
- [ ] 单维度缺失按显式降级规则处理（有测试用例覆盖）。

### 明确不做
- 不自训打分模型（1.0 用 LLM prompt；自训模型属后续阶段）。
- 不做同步打分——打分在 `memory.record` 返回路径之外（M5 返回时机不变）。

---

## 16. M15 — 写入链 worker（事件 → 图节点）

### 目标
落地"打分结果 → RMS 图节点"的写入路径，v1.0 中该链路无责任模块，本模块补齐。

### 实现要求
- **形态**：Pulsar consumer，消费 M14 的打分结果。
- **建顶点**：按 M2 schema 建 Event-Node——`c_i`/`τ_i`/`A_i`/`ref_ex` 全字段落位；φ_i 初始化：`s` = SS 合成初值、`n_created` = `n_last_touched` = 事件 n 序号、各计数器置 0。
- **建边**：时序边（同 space 内按时间序连接前序节点，immutable）。
- **向量**：写入 `rms_vectors`（routing = `space_id`），`node_key` 与图顶点关联。
- **关联与归属**：`ref_ex` 指回 EX 原始事件 ID；`agent_actor_id` 写入 `A_i` 的来源链路必须可信——**EX 摄入端点在落库时把凭证 claim 解出的 `agent_actor_id` 盖进事件的可信元数据字段**（摄入层职责，随 EX 摄入实现落地），本 worker 只从该元数据字段读取，禁止从事件体自由文本读取（事件体内容可被写入者伪造，元数据字段由摄入层盖章、写入者无法控制）。
- **幂等**：同一 EX 事件重复消费不重复建点（以 `ref_ex` 唯一性判定），重复投递不产生重复顶点/重复时序边。
- **n 连续性校验（v1.2 定案，消费侧自愈）**：消费时校验该 space 事件 n 的连续性，发现缺口即 page 级告警并触发补偿（按 n 区间从 EX 重放补发）——与 M10 Pulsar 生产侧定案配套，覆盖"EX 落库成功但发布失败/进程崩溃"的耐久性缺口，不新立轮询组件。
- **事件序号**：经验事件推进 n（消费时继承 EX 分配的序号）；元事件不推进 n（M6 规则在此执行）。

### 验收标准
- [ ] 端到端：`memory.record` 写入的事件，异步处理后成为符合 M2 schema 的完整顶点（字段级断言，含 φ_i 初值正确）。
- [ ] 幂等测试：同一事件重复投递 N 次，图中只有 1 个顶点、1 条时序边。
- [ ] 元事件（reinforce 追加）经过本链路时不建经验顶点、不推进 `n_now`。
- [ ] 向量写入与图顶点一一对应（抽样校验 `node_key` 关联），跨 space routing 隔离测试通过（复用 M2 零泄漏用例）。

### 明确不做
- 不建语义/因果/实体边——那是 consolidation worker 的慢路径职责（M2/M3）。
- 不自训 embedding 模型（1.0 用现成 embedding 服务/模型，选型进决策留痕）。

---

## 17. M16 — IS 简版（身份与凭证服务）

### 目标
落地账号与凭证的最小可用集，支撑 M5 鉴权设计与 M10 开通流程的调用入口。v1.0 中 IS 职责散落在各模块，本模块收口。

### 实现要求
- **账号 CRUD**：账号 → N 个记忆空间的归属关系（PostgreSQL 存储，见 M1）。
- **空间创建入口**：接收创建请求，调用 M10 开通流水线（三存储生命周期），返回 space_id；空间状态（active/destroying）与调度器映射联动。
- **JWT 签发与吊销**：
  - claim 结构：`account_id / space_id[] / agent_actor_id / scope`（与 M5 鉴权设计一致）；
  - 每个写入者身份单独签发凭证；
  - 支持吊销（吊销列表或短时效 + 刷新机制，选型进决策留痕）。
- **`debug` scope 管控**：`debug` scope 仅内部签发，C 端产品凭证一律不授予（M5 要求的签发侧落实）。
- **对接授权注册表**（§12.4，M0 建表）：训练数据授权的登记/查询/撤回入口在 IS 侧。

### 验收标准
- [ ] 账号 → 空间 → 写入者凭证三级关系完整，凭证 claim 字段与 M5 表一致。
- [ ] 空间创建请求触发 M10 开通流水线且顺序正确；创建失败不产生半开通状态（回滚校验）。
- [ ] 吊销后的凭证在受保护接口上被拒绝（有测试用例）。
- [ ] C 端凭证申请 `debug` scope 被拒绝；内部签发的 `debug` 凭证可在 `memory.retrieve` 取回 φ_i 内部字段（与 M5 验收项联动）。

### 明确不做
- 不做用户画像、偏好建模（那不是身份服务）。
- 不做 SSO / 第三方登录联邦（1.0 自有账号体系即可）。

---

## 18. M17 — 运维操作面（CLI 优先，不建 Web 后台）

### 目标
显式定义 1.0 的运维操作形态：**Grafana（读）+ 运维 CLI（写）+ 决策留痕表单（痕）**。M9/M10 定义了迁移/注销等流水线的能力，但"人从哪里触发"此前无责任模块，本模块收口。

### 实现要求
- **运维 CLI 命令清单**（覆盖 M9/M10/M16 全部人工触发点，缺一不可）：
  - space 状态查询（映射、tier、水位、配额用量）；
  - 迁移触发（人工拍板的再平衡 / Cell 退役 / 跨集群迁移，三类触发入口）；
  - 整 space 销毁处置（接收用户请求 → 触发 M10 注销流水线）；
  - 授权撤回处置（触发 M11 撤回流程）；
  - tier 升降调整；
  - Cell 水位查看与新 Cell 筹备触发。
- **两条硬性约束**：
  1. 每条命令**必须绑定显式 space 列表 / cell_id 参数**（红线 1 在操作面的落实），不存在"对全部 space 执行"的全局命令形态；
  2. 每条命令执行**自动写决策留痕**（操作人、命令、参数、结果），**无留痕能力的命令不得上线**——处置类操作（销毁/撤回）与留痕是原子要求。
- 监控读取面由 M12 的 Grafana 承担，本模块不重复建设；告警接收方式（邮件/IM webhook）选型进决策留痕。

### 验收标准
- [ ] CLI 命令清单覆盖 M9（迁移/水位/tier）、M10（注销）、M11（撤回处置）的全部人工触发点，逐条可执行。
- [ ] 静态检查：CLI 中不存在无 space/cell 绑定的全局操作命令。
- [ ] 执行任一处置类命令后，决策留痕表单中可查到完整记录（操作人/参数/结果），缺一视为未完成。
- [ ] 销毁处置端到端演练：模拟用户销毁请求 → CLI 触发 → M10 流水线执行 → 留痕可查 → 无残留校验通过。

### 明确不做
- **不建 Web 管理后台**——1.0 运维低频，CLI + runbook 足够；Web 后台归 2.0（届时由服务商运维场景与商用工单流的真实需求决定形态），且不引入前端技术栈（守 Python 统一定案）。
- 不做面向 C 端用户的自助控制台（C 端产品的自有后台由 C 端产品方建设，不在本服务范围）。

### M17 实施定案（修订记录第 25 条）
- 新包 `ops/ops_cli`（`lethefield_ops_cli`）为唯一运维写入口；九条命令：`space status/destroy/set-tier`、`migrate rebalance/to-cell/evacuate`、`auth revoke`、`cell watermark/register`，全部必选 `--space`/`--cell` 绑定（无全局形态），静态检查为 parser 内省单测。
- 留痕包装单点 `lethefield_ops_cli.audit.run_with_audit`：PG 预检 fail-closed → 执行 → `DecisionLogStore.submit`（操作人/命令/参数/结果；`outcome` 恒 `accepted`，成败记 `context.result`）；留痕库不可达拒绝执行，业务已执行但留痕失败退出码 2 + 人工补录提示。
- tier 升降走 `MappingTableControlPlaneStore.update_space_tier`（本模块新增的映射表扩展方法）；新 Cell 筹备走 `cell register`（映射行登记，基础设施由运维自备）。
- scheduler/training/is 既有 CLI 保留为底层入口；配额用量展示为近似值（图计数 TTL 缓存语义，输出注明）。
- 告警接收方式（邮件/IM webhook）选型为人工动作：用 `lethefield_decision_log submit` 记录选型决策，不在本模块写代码。

---

## 19. 验收总览（跨模块汇总，用于阶段 1 准出判定）

阶段 1（1.0 技术落地验证）准出条件，全部满足才能进入种子用户/烟测期：

- [ ] M0 工程地基验收通过（一键起栈 + CI 基线绿 + 决策留痕/授权注册表可用）。
- [ ] M1–M10 各模块验收标准全部通过。
- [ ] M14（SS 打分）、M15（写入链）、M16（IS 简版）验收标准全部通过——含 SS 打分稳定性验证报告入决策留痕。
- [ ] M17 运维操作面验收通过（CLI 覆盖全部人工触发点、无留痕命令不存在、销毁处置端到端演练通过）。
- [ ] M11 训练管线最小集（入料口①②③、R1–R3、热层落盘、撤回/销毁联动全部）完整可用。
- [ ] M12 开发期最小集指标全部可查询。
- [ ] M13 六条红线均有自动化检查手段，不是人工承诺。
- [ ] 从 EX 完整重放重建 RMS 全部状态的脚本可执行且通过校验（M7 验收项，同时是灾难恢复能力证明）。
- [ ] 混沌工程测试计划已制定（具体故障场景清单、演练节奏——由团队制定，不在本文档中锁定具体清单，但必须存在书面计划）。
- [ ] §11.3 决策留痕机制已在开发环境跑通，具备从第一天启动的能力（不要求已积累数据，要求机制可用）。

**不属于阶段 1 准出条件（阶段 2 事项，明确排除）**：
- 第三方服务商部署规范与一致性认证套件。
- per-space 加密。
- 封存层阈值自动化完整版、Premium 全常驻档商业化。
- 训练数据服务商场景授权结构。

---

## 20. 待实测 / 待标定参数一览（开发时使用默认/占位值，不要自行定最终值）

以下参数需在实测/种子期数据回填后标定，开发阶段先用配置项占位，**不要硬编码具体数值到业务逻辑中**：

- FF 相关：`λ`、`N_neglect`、`θ_base`、归档宽限期 `grace_n`（**事件距离度量**，见 M6；不是墙钟秒数）、固化阈值（`reinforce_count` 阈值）。
- DMS 新鲜度窗口：`W`（活跃判定窗口）与超窗告警阈值（见 M10 Dead Man's Switch 定案）。
- R3 关联时间窗 `W_r3`（召回明细 × 纠错事件的命中窗口，见 M11 R3 检测机制）。
- SS 六维合成权重（ER/E/I/G/N/C → `s` 初值）：**禁止硬编码**，以配置占位，待种子期真实数据标定（见 M14）。
- 各 `ff_*` 指标健康区间。
- Cell 容量标定（单 Cell 真实承载上限）、水位阈值 70/90（初值）。
- Pulsar 粗粒度分片阈值（namespace 数/backlog/跨地域）。
- EX 集群密度上限（append-only 低频读形态下 keyspace 上限能否突破 2000）。
- 写入链前序节点查询（per-space 图内按 `n_created` 扫描取前驱）的规模上限与索引需求（M15，1.0 无索引可接受）。
- 训练管线：授权注册表归属细节（并入调度器映射表 vs 独立表）、模型预标注启用时机。
- 离线聚合任务载体（轻量定时 worker，日更起步）。

---

*本文档基于设计文档 v1.7 转译。若开发过程中发现设计文档未覆盖的分支或矛盾，先升级确认，更新设计文档后再更新本开发文档，不要在开发文档中单方面添加未经设计层确认的架构决策。*
