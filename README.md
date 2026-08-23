# Lethefield — 1.0 开发 monorepo

FF（遗忘函数）驱动的记忆状态场：EX（经验事件流，唯一 SoT）+ RMS（关系记忆空间，
可重放重建）。设计定案见《Lethefield-设计文档》v1.7，开发执行依据见
《Lethefield-开发文档》v1.2（含修订记录第 1–27 条）。

**当前状态：M0–M17 全部完成，阶段 1 技术落地验证已准出（2026-08-15，
tag `stage-1-exit`，见《准出评审报告-v1_0.md》）。下一阶段：种子期，
计划见《规划-种子期计划-v0_1.md》。**

## 快速上手

前置依赖：Docker（macOS 用 colima，≥10GiB）、[uv](https://docs.astral.sh/uv/)。
CI/演练机（Tailscale `ubunturay`）环境见《环境-物理机-ci-runner-v0_1.md》。

```bash
bash scripts/ci.sh   # = lint + 单测 + 红线静态扫描 + 起全栈 + 集成测试（M1–M17 验收）
```

或分步：

```bash
make lint    # ruff 静态检查
make test    # 单元测试（libs + ops + services，不需要全栈）
make up      # 起单节点全栈并等待就绪
make itest   # 集成测试（需要全栈；建议先 make reset，历史 keyspace 累积会拖垮 Cassandra）
make down    # 停栈
make reset   # 清数据卷重起（危险：清空本地全部数据）

# M10 迁移演练（按需，不占常驻内存，默认 CI 跳过）：
docker compose --profile cell2 up -d && bash scripts/wait_for_stack.sh
uv run pytest tests/integration/test_m10_migration_drill.py -v        # 本地档（跨 Cell）
docker compose --profile cell2 --profile ex2 up -d && bash scripts/wait_for_stack.sh
uv run pytest tests/integration/test_m10_migration_drill_exit.py -v   # 准出档（含 EX 跨集群 sstableloader）
```

## 对外接口（用户接入）

**正式对外契约只有 HTTP + JSON API**（设计文档 v1.8 收窄定案；客户端 SDK/宿主插件/
宿主适配器属前端工程，见《前端设计-客户端与宿主接入-v0_1.md》）。

- **HTTP API（正式契约）**：FastAPI 四端点（`memory.record` / `flag_conflict` /
  `reinforce` / `retrieve`），Bearer JWT（HS256，凭证由 IS 签发/吊销）。
- **MCP server（本地/开发便利形态，非正式契约）**：
  `uv run python -m lethefield_api.mcp_server`（stdio 传输，token 走
  `LETHEFIELD_MCP_TOKEN` 环境变量）——四工具薄壳，业务逻辑全在 service 层。
- **Python SDK**：`lethefield_api.sdk.MemoryClient`（httpx 薄封装）。
- 凭证管理（账号/空间/签发/吊销/训练授权）走 IS CLI：
  `uv run python -m lethefield_is --help`；运维操作（销毁/迁移/tier/水位）走
  `uv run python -m lethefield_ops_cli --help`（九条命令，全部强制 `--space`/`--cell`
  绑定 + 自动决策留痕）。
- 完整命令清单见 `AGENTS.md` 常用命令表。

## 仓库结构

```
libs/logschema/    结构化日志事件 schema（M12 日志管线原料）
libs/metrics/      指标 registry 封装（命名规则 + 标签白/黑名单代码层强制）
libs/clients/      存储/Pulsar 客户端封装 + ControlPlaneStore 抽象（M0 冻结接口）
                   + 契约 1/3/5 + 授权注册表/凭证 store + FF/归档/迁移共享原语
services/          服务进程边界：api（M5 对外 API 层，HTTP+JSON；附 stdio MCP 薄壳，
                   本地/开发便利形态、非正式契约，设计文档 v1.8）/ fs（M6 sweep）/
                   rms（M2–M4/M7：schema/FF/检索/纠错/重建）/ scheduler（M9/M10 调度器）/
                   training（M11/M12 训练数据管线 + ③ 过滤器）/ ss（M14 六维打分）/
                   writer（M15 写入链）/ is（M16 身份与凭证）
ops/               decision_log（决策留痕）/ auth_registry（授权注册表薄 CLI）/
                   clock_monitor（红线 6）/ ingest_dms（M10 四路 DMS）/
                   metrics_exporter（M12 离线聚合）/ ops_cli（M17 运维操作面）
tests/integration/ spike q1–q4 CI 基线 + M1–M17 各模块验收测试
scripts/           巡检脚本：verify_isolation / check_graph_config / check_rms_schema /
                   check_space_model / check_space_filter / check_redlines /
                   verify_metrics_queryable / check_es_snapshot / rebuild_fidelity_drill 等
deploy/            prometheus / grafana provisioning + baselines/（冒烟/稳定性/重放/
                   迁移演练/指标验证等实测记录）
docker-compose.yml 单节点全栈：JanusGraph + Cassandra×2（cell/ex）+ ES×2 + Pulsar +
                   Redis(AOF) + PostgreSQL + Prometheus(9090) + Grafana(3000)
                   （+ cell2 / ex2 profile：迁移演练用第二 Cell / 第二 EX 集群，按需起）
```

## 文档地图

```
Lethefield-设计文档.md        决策层（v1.7，一切设计结论以此为准）
Lethefield-开发文档.md        执行层（v1.2：M0–M17 实现要求/验收标准 + 修订记录 1–27 + §20 待标定参数）
准出评审报告-v1_0.md          阶段 1 准出评审（§19 十条全过 + 官宣记录）
规划-种子期计划-v0_1.md       下一阶段总计划（前置工程/运营准备/参数标定/风险）
规划-混沌工程测试计划-v0_1.md  7 故障场景 × 注入/告警/自愈/恢复校验
任务划分-1.0开发-v0_1.md      五人协作接口表（历史）
课题-*.md                     挂起课题（集群池调度器 / 记忆治理层 / 训练管线设计等）
研究-认知动力学-3.0蓝图-v0_1.md 3.0 认知场理论研究存档（EX 是跨架构资产）
环境-物理机-ci-runner-v0_1.md  CI/演练机验收与移机记录
运维-runbook-ES快照备份恢复-v0_1.md 检索面灾难恢复运维前提（修订记录第 25 条）
工作日志.md                   逐模块实施记录与已登记缺口
AGENTS.md                     工程会话环境事实库（已验证事实，勿凭印象推翻）
```

## 硬约束（违反即评审不通过）

- 共享库只放 `libs/` 三样，禁止各服务重复造轮子；服务 = 进程边界。
- 所有存储访问必须经 `ControlPlaneStore` 抽象，禁止绕行直连。
- 聚合指标标签禁止出现 `space_id` / `node_key`（libs/metrics 代码层强制）。
- 六条多租户工程红线（设计文档 §11.5）有自动化检查（`check_space_filter` /
  `check_redlines`，已接 CI），不是人工承诺。
- EX 是唯一 SoT：RMS 全部状态可从 EX 重放重建（保真校验精确相等，
  `deploy/baselines/rebuild_fidelity_v1.md`）；向量检索面恢复 = ES 快照运维前提。
- 不执行 git commit / push，除非用户明确要求。
