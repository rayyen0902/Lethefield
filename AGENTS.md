# AGENTS.md

## 项目阶段

M0–M11（工程地基 / 存储基础设施 / RMS 图 Schema / FF 引擎 / 检索流程 / MCP·SDK 接口层 /
FS sweep worker / 纠错机制 / 记忆空间模型与鉴权 / Cell 架构 + 租户调度器 /
EX 存储与 Pulsar 归属 + 三存储生命周期流水线 / 训练数据管线）已完成并验证
（M0–M11 CI 全绿）。
**下一个模块：M12 可观测性埋点（开发期最小集）**（开发文档 §13：§19.3 系统指标 +
标定线最小集 + 日志事件 schema 落管线；M11 召回明细的进程内授权闸门是过渡形态，
日志管线上线后过滤器改读管线）。
一切设计结论以《Lethefield-设计文档》v1.7 为准，开发执行以《Lethefield-开发文档》v1.2 为准；
设计未覆盖的分支先升级确认，不自行拍板。

## 已验证的环境事实（不要凭印象推翻）

- JanusGraph 1.0.0 `ids.authority.wait-time` 默认值实测 **300ms**（management toString = PT0.3S）。
- kNN 跨 space 零泄漏 = `routing`（分片收拢）+ `space_id` 过滤（语义隔离）双重机制，
  只带 routing 会泄漏（分片是多 space 共享的）——M4 Stage 2 实现必须沿用，见 tests/integration/conftest.py。
- Gremlin 绑定名避开保留字（`keys` 等）；返回值在服务端按元素逐个流回，不是嵌套列表。
- gremlin_python 把 Python int 一律按 int32 序列化，毫秒时间戳等大数会溢出——Long 属性绑定用
  字符串传输 + Groovy `as long` 强转（范例见 services/rms writer.py，M3/M15 沿用）。
- M2 RMS schema 定案：**17** 个顶点属性键（v1.2 固化定案新增 `consolidated_at`：存在即固化态，M4 两处 θ 硬过滤对其旁路，固化时 `n_star_cached` 置 LONG_MAX）/ 5 边标签（temporal immutable）/ `byNodeKey` 唯一复合索引
  + `byEntityKey` 非唯一；向量走 ES 独立索引 `rms_vectors`（`node_key` 关联图顶点、`space_id` routing）。
  schema 常量单点定义在 `services/rms`（lethefield_rms.schema），groovy 脚本只含逻辑、元素名经绑定传入；
  `writer.py` 是 M15 写入链地基（φ 初始化：n_last_touched=n_created、三计数器 0）。
  `ref_ex` 校验目前只覆盖 RMS 侧不变量，EX 侧 join 待 M10。
- JanusGraph 顶点不支持 `v.both(Direction, label)` 直调，邻居扩展用 traversal 的 `both()` 步骤。
- M3 FF 引擎定案：公式与 δ 逻辑单点在 `lethefield_rms.ff`（禁止其他模块内嵌 FF 公式副本，
  tests/integration/ff_utils.py 也只是它的薄委托）；`n_star_cached` 存**绝对遗忘视界**
  （n_last_touched + ceil(n*)，粗筛 `> $n_now` 才成立），任何 δ 调整立即重算；s 合法区间
  [0,1]（设计未明文，FFConfig 可配）；截断必计 `lethefield_ff_s_clamp_total{bound}`。
- `libs/metrics` 的 `registry=None` 是 prometheus_client 原义——**不注册**；模块级指标要显式传
  `prometheus_client.REGISTRY`（服务暴露口 M12 统一接线）。
- M4 检索定案：`lethefield_rms.retrieve` 四阶段（RRF 不掺 s、两次独立硬过滤不合并）；
  `_stage3_traverse` **签名无 es 无 rho**——"Stage 3 不访问 ES"与"ρ 不影响软惩罚"靠签名物理隔离；
  λ1·φ = 边类型先验（占位 causal 1.0/semantic 0.8/temporal 0.6，实体边不作扩展路径、实体叶子收敛后挂回）；
  λ2·sim 方案 A（锚点=RRF 分、扩展节点=0；方案 B 继承父 sim 衰减留待效果验证后探索）；
  占位参数集中在 `RetrieveConfig`。`rms_vectors` 已加 `content` 文本字段（Stage 2 关键词一路）。
  writer 的 `n_star_cached` 默认自动按 `n_star_horizon` 计算（传 0/不填会让粗筛全灭召回）。
- M5 接口层定案：`services/api`（lethefield-api）。契约 1/3 已在代码冻结——EX 事件两表
  （`ex_{space_id}` keyspace：experience_events 经验事件推进 n / meta_events 元事件不推进），
  JWT claims `account_id/space_id[]/agent_actor_id/scope[]`（HS256 + env LETHEFIELD_JWT_SECRET，
  签发属 M16）；请求体带 `agent_actor_id` 一律 400 actor_spoof（fail-closed）；图名 = space_id；
  错误码 `{error:{code,message}}`（unauthorized/forbidden_scope/forbidden_space/actor_spoof/
  bad_request/not_found/rate_limited/internal）；FF 内部字段一律 debug scope 才出（含 reinforce 响应）。
  space_id 命名约束 [a-z0-9_]≤40（EX keyspace，fail-closed 不改写）。
- FastAPI 端点必须 sync def（不能 async）：gremlin_python 同步客户端内部 run_until_complete，
  在事件循环里直接冲突；sync 端点由 FastAPI 放线程池执行。请求体用 `Annotated[dict, Body()]`。
- M6 FS 定案：`services/fs`（lethefield-fs）。space 枚举走 `ControlPlaneStore.list_spaces()`
  抽象（过渡期 `ExKeyspaceControlPlaneStore` 从 EX keyspace 元数据推导，M9 切映射表零改动）；
  n_now/keyspace 命名约定单点在 `libs/clients/ex_n.py`（ex_ingest 薄委托）；
  归档副本写本 space RMS keyspace 内 `archived_nodes` 表（直写 CQL 不经 JanusGraph，
  表名与访问封装在 `libs/clients/archive.py`）；归档宽限期 = 事件距离 `grace_n`
  （FFConfig 占位 40），资格判定纯函数 `ff.archive_eligible`（M7 重建必须复用）；
  固化判定先于归档；sweep 心跳 `fs:sweep:last_ok`，巡检 `python -m lethefield_fs.liveness`。
  固化后 δ 不改 s、计数器照计（ff.compute_delta 单点）；物理删除不在 FS（整 space 销毁归 M9/M10）。
- gremlin_python 反序列化的 Date 是 **naive datetime**（UTC 语义）：`.timestamp()` 前必须
  先 `replace(tzinfo=UTC)`，否则按本地时间解释（M6 归档快照踩坑）。
- M7 纠错定案：`lethefield_rms.corrections` 扫 EX `ref_conflict` 事件，**单 tx 原子幂等**
  （tx 内查 supersedes 边，已存在 → duplicate 零写入——边即幂等标记，无状态表）；新节点按
  `ref_ex=event_id` 反查、旧节点按 `node_key=ref_conflict`，缺失即 pending。reinforce 时间窗合并
  在 ex_ingest（`REINFORCE_MERGE_WINDOW_MS` 占位 60s，同主键 UPDATE 累加 count，不动契约 1 表结构）。
  `lethefield_rms.rebuild` 从 EX 重放重建：初始 s 走注入 `s_resolver`（默认占位 1.0，M14 切
  `scoring_result` 元事件——两档验收已入档）；node_key 由 `node_key_of` 单点生成（过渡约定，M15 对齐）；
  忽视/固化/归档理想化 sweep 重推，`neglect_due`/`consolidate_due`/`archive_eligible` **单点在 ff**
  （已从 fs/sweep 迁入，sweep re-export）。重建边只建双端在热图的（归档节点不落图，addEdge 会炸）；
  重建图顶点 space_id = 源 space，与目标图名不同。
- M8 空间模型定案：space_id 字符集 **[a-z0-9_]≤40 正式定案**，单点 `libs/clients/spaces.py`
  `validate_space_id`（EX keyspace / 图名 / ES routing / Pulsar namespace 四处命名共用；
  `ex_n.keyspace_name` 只是委托）；API 四操作入口 `_require_valid_space` fail-closed
  （非法 space_id 400 bad_request 零副作用）。`SpaceType`（companion/project）+
  `SpaceMapping.space_type` 可选注解——仅产品/运营标注，核心服务禁止引用/分支，
  `scripts/check_space_model.py` 静态巡检强制（含 agent_id 分区键残留扫描，已接 ci.sh）。
  集成测试的 JWT 密钥（LETHEFIELD_JWT_SECRET 是进程级 env）必须在模块 fixture 里设定——
  import 时设定会被后导入模块覆盖，先执行模块 token 全 401（M8 实测踩坑）。
- M9 Cell 架构定案：`services/scheduler`（lethefield-scheduler）。映射表 =
  `lethefield_control` keyspace（cassandra-cell 上专用 keyspace，直写 CQL 不经 JanusGraph；
  spaces/cells 两表），正式实现 `MappingTableControlPlaneStore`（过渡期
  ExKeyspaceControlPlaneStore 已删除；`list_spaces` 按 status=active 过滤，FS/纠错调用方
  零改动）。备份/导出单点 `libs/clients/control_backup.py`（JSONL 全量含非 active，
  restore 覆盖写幂等，1.0 验收硬指标）。计算侧 `MappingCache`（TTL + **控制面故障服务
  陈旧值**，负缓存语义同）——四操作经 `_resolve_gname` 解析，未注册 space 404 fail-closed。
  开通顺序 EX → Pulsar → RMS 建图+schema → 注册（先存储后注册，失败逆序回滚）；
  建图 backend props 按 Cell endpoints 推导（`lethefield_rms.schema.backend_props_of`，
  ensure_graph_schema 第三参注入）。注销五步严格按序：标 destroying →
  **close + removeConfiguration 驱逐计算实例（红线 5，先于任何 DROP）** → rms_vectors 文档
  → DROP 图 keyspace → 删 namespace → 最后 DROP EX keyspace → 广播（`broadcast_destroy`
  注入点，M10 接训练管线真接口）→ 清映射 + 无残留校验。水位 0.7/0.9 初值（SchedulerConfig），
  纯函数 `state_of` + 注入式 `refresh_cell`（disk/heap 单节点无探针置 0，模拟负载走注入）。
  Pulsar namespace 经 admin REST（tenant 固定 `lethefield`，namespace = space_id）。
- compose 里 Cassandra 必须显式设 `CASSANDRA_BROADCAST_RPC_ADDRESS`（官方镜像重启后会失效）。
- 测试图只 close 不 DROP 会累积 keyspace/table 拖垮 Cassandra schema 操作（实测 51 keyspace/463 表时
  图创建连锁超时）——套件莫名变慢/超时先 `make reset` 再怀疑代码。
- bind mount 内容变更不触发 compose recreate，`docker compose restart <svc>` 才重读配置；
  JanusGraph 已配 `graph.replace-instance-if-exists=true` 与 `evaluationTimeout: 180000`
  （容器重建撞实例注册冲突 / 冷图创建在全量负载下超默认 30s，两处实测）。
- M10 定案：契约 5（训练管线销毁指令）单点在 `libs/clients/training_control.py`
  （`space_ref_of` 不透明哈希 M11 样本 schema 复用；topic = 训练 tenant
  `lethefield-training/control/space-destroy`，durable 订阅 `training-destroy-sink`）；
  destroy 第 4 步默认真实广播（等 broker ack，失败中止不进第 5 步，映射留 destroying 可重试）。
  DMS 在 `ops/ingest_dms`：监控 tenant probe topic 探针（page 级，**禁向 space namespace
  发探针**）+ 训练控制 backlog 停滞（page 级）+ space 写入新鲜度（observation 级，
  Redis `ex:last_write:{space}` 由摄入路径成功写入后维护，翻转边告警）。
  迁移流水线 `scheduler/migrate.py`：等价校验对齐"目标图 == EX 重放计划"（EX 唯一 SoT，
  源图可能含重放不覆盖的 consolidation 边）；写路径 migrating → 429 rate_limited
  （复用现有契约码，retrieve 放行）；本地档演练 = compose `cell2` profile（按需起、
  默认不进 CI，heap ~1.3G 注释锁死），实测只读窗口 15.6s（记录
  `deploy/baselines/m10_migration_drill.jsonl`）；EX 跨集群 sstableloader 与 API 多 Cell
  连接路由是已登记缺口。
- M11 定案：训练数据管线在 `services/training`（lethefield-training）。feed 信封与 topic
  单点 `libs/clients/training_feed.py`（topic `lethefield-training/feeds/raw`，短 retention
  **过境 ≠ 沉淀**；信封 kind 四值 + R1/R2 判定纯函数 `decision_rules` 单点——提交路径与
  worker 共用）；销毁订阅名单点 `DESTROY_SUBSCRIPTION` 在 `training_control.py`（M10 sink
  与 M11 worker 共用继承积压）。授权注册表 store 下沉 `libs/clients/auth_registry.py`
  （独立 Postgres 表，§12.4 待决项 1 按现状收口；ops/auth_registry 仅剩薄 CLI re-export），
  新增 `delete`（销毁处置删注册表项）。decision_log 补齐 §11.3 三列
  （agent_suggestion/outcome/escalation_type；幂等迁移 `deploy/postgres/migrations/001_*.sql`；
  R1 = outcome≠accepted、R2 = escalation_type 非空，命中才发布——常规流量不进管线）。
  ③ 入料口：API retrieve 发射最小化召回明细（space_ref + node_key 列表 + θ 阶段计数 +
  query 类别，**无 query 原文**），LogEvent 进运维日志 + CALIBRATION 授权闸门后入 feed
  topic（进程内闸门是 M12 日志管线上线前的过渡形态）；`RetrievalResult.stats`
  （anchors/pool/returned）是 θ 统计数据源。④ 入料口 `ex_feed`：只读 EX 派生纠错对
  （旧内容按 `ref_conflict` 去 `ev_` 前缀反查 EX 事件，不触 RMS），CONTENT_COPY 闸门
  入 topic 前拒发，state 文件幂等。worker 双订阅轮询（feed `training-sample-worker` +
  control `training-destroy-sink`），worker 侧授权复查为第二道防线；热层 =
  本地目录（默认 `var/training`）：`hot/samples-日期.jsonl` + `index/{space_ref}.jsonl`
  清单索引，`scrub` 只读目标清单按单重写（**O(清单)**，撤回/销毁同一处置），骨架保留
  内容字段清空。R3 = 召回明细 × 纠错对按 node_key + `W_r3` 窗关联（默认 24h 占位，
  env 可配待标定），**未经召回的纠错不计入 R3**（归 R4/R6 再议）；召回窗落
  `recall_window.jsonl` 重启可重建。worker 侧模块禁 import gremlin/cassandra/ex_n
  （红线 1，静态测试 `test_no_business_db.py` 强制）。指标白名单新增
  source/rule/review_status/kind 四个低基数枚举标签。
- Pulsar Exclusive 订阅在前一 consumer 关闭后短暂窗口内报 ConsumerBusy（broker 连接回收
  滞后）——快速重订阅要退避重试（worker `_subscribe_with_retry`），常驻形态订阅常开
  不按轮重建（M11 集成测试实测）。
- pulsar-client 的 `receive` 参数名是 `timeout_millis`（写错被宽泛 except 吞成假阴性——
  consumer 循环只捕 `pulsar.Timeout`）；Pulsar 412：backlog quota 必须 < retention size。
- **gremlin_python 客户端基于 tornado 单连接，跨线程共享同一 Client 会死锁**——
  并发线程各用各的连接（M10 迁移演练实测挂死根因）。
- spike 遗留容器（spike-elasticsearch 等）已停止但未删除，端口 8182/9042/9200 若被占先检查它们。
- colima VM 内存不足会被内核 OOM killer 杀 JanusGraph（exit 137，dmesg 可见
  "Out of memory: Killed process (java)"）——表现为 gremlin 连接拒绝、后续模块连锁
  ERROR。宿主机 16GB 下 VM 已从 8GiB 调到 **10GiB**（M9 全量 CI 实测踩坑，2026-08-06）；
  再发生先看 dmesg，别先怀疑代码。

## 常用命令

| 命令 | 作用 |
|---|---|
| `bash scripts/ci.sh` | 本地 CI 全流程（lint + 单测 + 起栈 + 集成测试） |
| `make lint` | ruff check + format check |
| `make test` | 单元测试（libs + ops + services，不需要全栈） |
| `make up` / `make down` / `make reset` | 起栈 / 停栈 / 清卷重起 |
| `make itest` | 集成测试（需全栈已就绪） |
| `uv run python scripts/verify_isolation.py` | M1：物理隔离证明巡检 |
| `uv run python scripts/check_graph_config.py` | M1：`ids.authority.wait-time` 默认值巡检（红线 4） |
| `uv run python -m lethefield_clock_monitor` | M1：时钟偏移巡检（红线 6），超阈值退出码 1 告警 |
| `uv run python -m lethefield_rms.schema <gname>` | M2：初始化/补齐某 space 图的 RMS schema（幂等） |
| `uv run python scripts/check_rms_schema.py --graph <gname>` | M2：schema 巡检 + rms_vectors mapping + ref_ex 抽样 |
| `uv run python -m lethefield_fs [--once]` | M6：FS sweep worker（--once 单轮，测试/巡检用） |
| `uv run python -m lethefield_fs.liveness` | M6：sweep 存活性巡检（Dead Man's Switch），超窗口退出码 1 告警 |
| `uv run python -m lethefield_rms.corrections [--space S]` | M7：纠错处理器单轮（扫 EX ref_conflict → supersedes 边 + −0.5，幂等） |
| `uv run python -m lethefield_rms.rebuild <space> [--target-gname G]` | M7：EX 重放重建 RMS（图结构 + δ 历史 + supersedes + archived_nodes） |
| `uv run python scripts/check_space_model.py` | M8：空间模型巡检（agent_id 分区键残留 + 核心服务 space_type 引用扫描，不起栈） |
| `uv run python -m lethefield_scheduler bootstrap` | M9：建控制面表 + 注册本地 Cell（幂等） |
| `uv run python -m lethefield_scheduler provision <space> [--tier T]` | M9：开通 space（EX → Pulsar → RMS → 注册，失败回滚） |
| `uv run python -m lethefield_scheduler destroy <space>` | M9：注销 space（先驱逐计算实例再删存储，无残留校验） |
| `uv run python -m lethefield_scheduler export <file>` / `restore <file>` | M9：映射表备份 / 恢复（1.0 验收硬指标） |
| `uv run python -m lethefield_scheduler watermark [--cell ID]` / `list` | M9：水位刷新 / 映射一览 |
| `uv run python -m lethefield_scheduler migrate <space> [--to-cell ID]` | M10：跨 Cell 迁移（实测只读窗口入报告） |
| `uv run python -m lethefield_scheduler.training_control_sink [--once]` | M10：契约 5 销毁指令最小接收 consumer |
| `uv run python -m lethefield_ingest_dms [--once]` | M10：EX 摄入 DMS 巡检（探针/backlog/新鲜度），page 告警退出码 1 |
| `uv run python -m lethefield_training worker [--once]` | M11：加工 worker（feed 消费 + 契约 5 销毁处置 + 热层落盘） |
| `uv run python -m lethefield_training scrub <space_ref>` | M11：撤回授权存量处置（O(清单) 定位、内容清除、骨架保留，幂等） |
| `uv run python -m lethefield_training submit-incident --problem P --diagnosis D --decision C --outcome O` | M11：② 入料口（故障/混沌案例提交，rule=R5） |
| `uv run python -m lethefield_training ex-feed --space S` | M11：④ 入料口（EX 只读派生纠错对，CONTENT_COPY 闸门，幂等） |
| `uv run python -m lethefield_decision_log submit ... [--feed]` | M11：决策留痕提交（三列已补；--feed 时 R1/R2 命中发布训练 feed） |
| `docker compose --profile cell2 up -d` 后 `uv run pytest tests/integration/test_m10_migration_drill.py` | M10：跨 Cell 迁移演练（按需，不占常驻内存；默认 CI 自动 skip） |

## 约定

- 升级确认边界：只有**设计未覆盖的分支**（语义、契约、schema、数据归属、红线相关）才升级参谋会话确认；环境重置、依赖微调、重构手法、测试组织等纯工程项自主决定（口诀："改错了要不要动文档？要动文档的升级，只动代码的自主"）。
- 集成测试前统一 `make reset` 清卷重起：dev 卷内测试图均为一次性产物，历史 keyspace 累积是超时/抖动来源；reset 后 schema 由 CI 流程自动重建。执行前确认无进行中的调试依赖卷数据、spike 遗留容器未占端口。
- Python 统一（uv workspace，>=3.12）；服务 = 进程边界，共享代码只允许 `libs/` 三样。
- 所有存储访问必须经 `libs/clients` 的 `ControlPlaneStore` 抽象，禁止绕行直连。
- 指标命名 `lethefield_<域>_<名称>_<单位>`；标签白/黑名单由 `libs/metrics` 强制，
  `space_id` / `node_key` 永不可作聚合指标标签（space 粒度明细走 `libs/logschema` 日志事件）。
- `tests/integration` 是 spike q1–q4 的 CI 基线：高分召回 / 低分过滤 / 衰减过滤 / 跨 space 隔离。
  改动检索、图 schema、向量索引相关代码时必须保持其四断言全绿。
- 红线（详见设计文档 §11.5）：禁止跨 space 全局扫描；`ids.authority.wait-time` 保持默认；
  禁止在线 DROP JanusGraph 使用的 keyspace；节点时钟同步是硬性前提。
- 不执行 git commit / push 等 git 变更操作，除非用户明确要求。
