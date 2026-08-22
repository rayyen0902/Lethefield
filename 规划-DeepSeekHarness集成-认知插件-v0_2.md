# 规划 — DeepSeek Harness（dsh）集成：Lethefield 认知插件 v0_2

**版本**: v0.2
**状态**: 待办（仅规划，未启动执行）
**日期**: 2026-08-23（v0.2 增补 §6 多 harness 适配策略）
**关联**: 《Lethefield-设计文档》v1.7（§0.5 价值观公理、§7 隔离）；《课题-dsh会话日志与EX事件流映射-v0_1》

---

## 0. 背景与意图

DeepSeek Harness（dsh，DeepSeek 官方开源的 coding agent 运行时，2026-08-13 开源，MIT，TypeScript）采用"一切皆插件"架构：模型、工具、会话、存储、上下文注入、Agent 主循环均为 Cordis 插件，能力经 Capability Seam（Definition / Provider / Consumer 三角色）替换。官方 Creator 模式已内置 memory 插件实验方向。

**意图**：不经 MCP，将 Lethefield 作为认知服务直接接入 dsh 运行时——dsh 的会话事件流作为 EX 摄入来源，Lethefield 的 RMS retrieve 替换/增强 dsh 的上下文注入环节。这是 Lethefield 的第一个真实宿主环境，也是"遗忘是功能"这一核心论点首个可公开演示、可评测的场景。

**战略定位**：本插件即 Lethefield 的最小可演示场景（MVD）。目标演示：同一 coding 任务在 30 天尺度上，启用 FF 认知层的 dsh 对比纯日志检索基线，在召回质量 / 噪声抑制 / token 成本上的差异。

## 1. 路径决策（已定）

| 路径 | 结论 | 理由 |
|---|---|---|
| A. fork dsh 源码直接改 | **否决** | dsh 处于 Developer Preview，官方声明将有破坏性变更且暂不接受外部 PR；fork 与 Lethefield"M0 冻结接口"的工程文化冲突 |
| B. dsh Python SDK 编排 | **否决** | Python SDK 用于驱动 Agent 跑任务（脚本自动化），不是运行时扩展点，够不着认知层 |
| C. Cordis 插件（TS 薄壳）+ HTTP 直连 Lethefield api 服务 | **采纳** | 即 Capability Seam 的设计用途；Lethefield 侧零改动（services/api M5 已就绪）；dsh 若换代，壳重写、本体无损 |

**红线**：不走 MCP 层；不改 Lethefield 本体代码；插件只做事件接线与 HTTP 调用。

## 2. 目标架构

```
dsh 运行时（Node/TS）
 └─ lethefield-plugin（Cordis 插件，薄层）
     ├─ 监听会话事件流 → POST 至 Lethefield EX 摄入（异步）
     ├─ 实现上下文注入 seam → 调 RMS retrieve，注入塌缩结果（同步）
     └─ space_id ← dsh workspace/session 一对一映射
          │ HTTP（本地或内网）
Lethefield 全栈（docker compose 单节点，Python 本体零改动）
```

## 3. 工程红线

1. **space 隔离**：dsh 的 workspace/session 与 Lethefield space_id 严格一对一映射；禁止多 workspace 共享 space（违反即触及 §0.5 公理 3 结构性越界）。
2. **延迟预算**：retrieve 位于每次模型请求的同步注入路径，Lethefield 全栈 P99 延迟将直接叠加到每轮对话。插件层必须：摄入异步化（写入后台），仅读取同步；预留本地缓存位。此点预计在 M13（多租户工程红线）之前最先撞墙。
3. **seam 依赖白名单**：插件只依赖 dsh 公开的 Capability Seam 接口与公开事件，禁止 import Cordis 内部实现——dsh 破坏性变更时损失限定在壳层。
4. **可关闭**：插件必须可整体禁用，dsh 回落到默认上下文组装——演示对比实验（有/无 FF）依赖此开关，也是故障回退路径。

## 4. 待办清单（未排期）

- [ ] T1：调研 dsh 插件开发文档与 Cordis Service/Context/Event 模型，确认会话事件流与上下文注入两个 seam 的公开接口签名
- [ ] T2：产出插件骨架设计（目录结构、事件监听清单、retrieve 注入点接口签名）
- [ ] T3：确定 space_id ↔ workspace/session 映射方案与落库位置（候选：ControlPlaneStore 映射表扩展）
- [ ] T4：EX 摄入接口对 dsh 事件 schema 的适配（事件字段映射表，见关联课题文档）
- [ ] T5：插件最小实现：摄入通路先行（只写不读），验证事件流完整性
- [ ] T6：retrieve 注入通路接通，同步路径延迟实测，定延迟预算数值
- [ ] T7：演示对比实验设计：30 天纵向任务流，FF 开/关两组，指标 = 召回质量 / 噪声率 / token 成本
- [ ] T8：故障回退演练：Lethefield 栈宕机时 dsh 的行为（应回落默认注入，不得阻塞会话）

## 5. 风险

- **R1（高）**：dsh Developer Preview 破坏性变更 → 缓解：红线 3 白名单 + 壳层极薄。
- **R2（高）**：同步 retrieve 延迟不可接受 → 缓解：T6 先行实测；退路为预取缓存 + 降级为异步提示。
- **R3（中）**：dsh 事件流字段不足以支撑 SS 六维显著性评分 → 课题文档跟踪；退路为首版 FF 仅启用事件视界距离维。
- **R4（中）**：30 天演示周期过长，短期 demo 中"遗忘"表现为疑似 bug → 缓解：T7 同时设计加速时间标尺（压缩事件密度的短周期变体）。

## 6. 多 harness 适配策略（v0.2 新增）

### 6.1 背景

2026-08-19，OpenAI 将 Codex / ChatGPT Work 底层 harness 开源（Apache-2.0，`openai/codex`，Rust 核心 + TS/Python SDK）。其架构与 dsh 不同：无 Cordis 式插件缝，对外暴露三层集成接口——`codex exec`（非交互）、Codex SDK（编程式任务生命周期）、**app-server（本地进程 + JSON-RPC 协议：持久会话、实时事件流、人工审批回调）**。harness 层正被各厂打到免费（DeepSeek MIT、OpenAI Apache-2.0、Block Berd），中间管道无定价权——Lethefield 的立足点在 harness 之上的认知增值层。

### 6.2 策略：宿主无关接入抽象

不为每个宿主写一套认知逻辑。在 Lethefield 侧定义**宿主无关接入抽象（Host Adapter Abstraction）**，只有两个接口：

```
接口 1：事件摄入  ingest(events)      ← 宿主事件流 → EX
接口 2：认知注入  retrieve(query_ctx) → 宿主上下文组装 ← RMS 塌缩结果
```

各宿主的适配器只是该抽象的实现，壳层独立仓库或独立目录演进：

| 宿主 | 适配形态 | 事件源 | 注入点 | 控制力 | 稳定性 |
|---|---|---|---|---|---|
| dsh | Cordis 插件（本规划 §2） | 会话事件日志流 | 上下文注入 seam（内部替换） | 强 | 低（Developer Preview） |
| Codex | app-server 适配器（JSON-RPC 客户端/中间层） | JSON-RPC 事件流 | 协议层旁路注入（改不了内部组装） | 弱 | 高（文档化标准协议） |

**实施顺序不变**：dsh 先行（可控上下文组装，是 FF 对比实验的必要条件）；Codex 适配器待 dsh 通路验证后启动。两个适配器共用同一套 ingest/retrieve 接口契约，事件 schema 差异收敛在适配器内，不进 Lethefield 本体。

### 6.3 新增待办

- [ ] T9：定义宿主无关接入抽象的接口契约文档（ingest / retrieve 签名 + 事件 schema 最小公约数）
- [ ] T10：Codex app-server JSON-RPC 协议调研：事件流字段、审批回调与 EX 事件的映射可行性
- [ ] T11：Codex 适配器立项评审（依赖：T7 dsh 对比实验出初步结果后）
