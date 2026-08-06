# Lethefield — 1.0 开发 monorepo

FF（遗忘函数）驱动的记忆状态场。设计定案见根目录《Lethefield-设计文档》v1.7，
开发执行依据见《Lethefield-开发文档》v1.2。**当前进度：M0–M9 已完成（CI 全绿），
下一模块 M10（EX 存储与 Pulsar 归属 + 三存储生命周期流水线）。**

## 快速上手

前置依赖：Docker（macOS 用 colima）、[uv](https://docs.astral.sh/uv/)。

```bash
bash scripts/ci.sh   # = lint + 单测 + 起全栈 + 集成测试（q1–q4 基线 + M1–M9 验收）
```

或分步：

```bash
make lint    # ruff 静态检查
make test    # 单元测试（libs + ops + services，不需要全栈）
make up      # 起单节点全栈并等待就绪
make itest   # 集成测试（需要全栈）
make down    # 停栈
make reset   # 清数据卷重起（危险：清空本地全部数据）
```

## 仓库结构

```
libs/logschema/    结构化日志事件 schema（M12 日志管线原料）
libs/metrics/      指标 registry 封装（命名规则 + 标签白/黑名单代码层强制）
libs/clients/      存储/Pulsar 客户端封装 + ControlPlaneStore 抽象（M0 冻结接口）
ops/decision_log/  决策留痕表单最小实现（§11.3）
ops/auth_registry/ 训练数据授权注册表最小实现（§12.4）
ops/clock_monitor/ 时钟偏移监控告警（红线 6，M1 部署清单硬性项）
services/          服务进程边界：api（M5 接口层）/ fs（M6 sweep）/ rms（M2–M4/M7）/ scheduler（M9 调度器）
tests/integration/ spike q1–q4 CI 集成基线 + M1–M9 各模块验收测试
docker-compose.yml 单节点全栈：JanusGraph + Cassandra×2 + ES×2 + Pulsar + Redis + PostgreSQL
```

## 巡检脚本（M1）

```bash
uv run python scripts/verify_isolation.py      # 4 类存储物理隔离证明（M1 验收 1）
uv run python scripts/check_graph_config.py    # ids.authority.wait-time 默认值核验（红线 4）
uv run python -m lethefield_clock_monitor      # 时钟偏移巡检，超阈值告警（红线 6）
bash scripts/measure_restart_baseline.sh       # 重启至可服务时间基线（手动，非 CI）
```

M2–M9 各模块巡检/运维命令（schema 巡检、FS liveness、纠错、重建、调度器 CLI 等）
完整清单见 `AGENTS.md` 常用命令表。

## 硬约束（开发文档 M0，违反即评审不通过）

- 共享库只放 `libs/` 三样，禁止各服务重复造轮子。
- 所有存储访问必须经 `ControlPlaneStore` 抽象（M0 冻结接口，M9 已落地映射表正式实现）。
- 聚合指标标签禁止出现 `space_id` / `node_key`（libs/metrics 代码层强制）。
- M0 不写任何 EX/RMS/SS/FS 业务逻辑。
