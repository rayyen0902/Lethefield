# AGENTS.md

## 项目阶段

M0–M17（工程地基 / 存储基础设施 / RMS 图 Schema / FF 引擎 / 检索流程 / MCP·SDK 接口层 /
FS sweep worker / 纠错机制 / 记忆空间模型与鉴权 / Cell 架构 + 租户调度器 /
EX 存储与 Pulsar 归属 + 三存储生命周期流水线 / 训练数据管线 / 可观测性埋点 /
多租户工程红线落地 / SS 显著性打分服务 / 写入链 worker / IS 简版 / 运维操作面）
已完成并验证（M0–M17 CI 全绿）。
**下一步：阶段 1 准出验收**（开发文档 §19 验收总览，跨模块汇总判定）。
一切设计结论以《Lethefield-设计文档》v1.7 为准，开发执行以《Lethefield-开发文档》v1.2 为准；
设计未覆盖的分支先升级确认，不自行拍板。

## 已验证的环境事实（不要凭印象推翻）

- CI/演练机已就位（Tailscale `ubunturay` 100.92.236.89，`ssh ray@ubunturay`；全量 CI 8m35s 全绿，见 `环境-物理机-ci-runner-v0_1.md`；该机 uv 直连 PyPI 会被重置，已配用户级 `~/.config/uv/uv.toml` 清华镜像，重装/换机需带上）。
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
  （`ex_{space_id}` keyspace：experience_events 经验事件推进 n / meta_events 元事件不推进；
  **M14 契约 1 首次演进：meta_events 加可空 `details` 列（text/JSON，按 meta_type 分型、
  schema 单点在 lethefield_rms.schema），演进规则 = 只允许可空加列式兼容演进**），
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
  `lethefield_rms.rebuild` 从 EX 重放重建：初始 s 走注入 `s_resolver`（**M14 起默认
  `ex_scoring_s_resolver` 读 scoring_result details——全保真档**；缺失回退占位 1.0 +
  emit `rebuild_scoring_missing`；`placeholder_s_resolver` 保留显式回退）；node_key 由 `node_key_of` 单点生成（过渡约定，M15 对齐）；
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
  import 时设定会被后导入模块覆盖，先执行模块 token 全 401（M8 实测踩坑）；**token 签发
  同理须在 fixture 内**——import 时签发的 exp 以导入时刻起算，套件变长后执行窗口拿到
  过期 token 同样全 401（M14 全量 CI 实测，m5/m8 已改 fixture 内签发）。
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
  topic（进程内闸门已在 M12 收口删除——定案形态 = recall_filter 读 es-ops 管线，见下）；
  `RetrievalResult.stats`（anchors/pool/returned）是 θ 统计数据源。④ 入料口 `ex_feed`：只读 EX 派生纠错对
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
- M12 定案：日志管线单点 `libs/logschema/es_sink.py`（`emit(event, sync=)`：stderr 恒留底 +
  configure 后异步批量进 es-ops `lethefield-logs-YYYY.MM.DD`；一次性 CLI 用 sync=True 惰性
  建 shipper 直写；ES 不可达 fail-open）。暴露口：API 自身端口 `/metrics`（不挂业务 scope，
  1.0 内网口径）；worker 进程 `lethefield_metrics.start_metrics_server`（端口约定 fs 9101 /
  training 9102 / ingest_dms 9103 / exporter 9104 / ss 9105，env `LETHEFIELD_METRICS_PORT`，
  0=不起，--once 不起）。离线聚合 = `ops/metrics_exporter`（只读 es-ops 日志流 + system.size_estimates
  + 映射表 + rms_vectors _stats/_count 元数据——红线 1 边界）：留痕线/δ counter 与
  graph_open histogram 从日志流折叠（游标进程内、重启全量重建）；**δ/留痕计数统一走
  日志流聚合通道**（一次性 CLI 进程内 counter 不可 scrape）。`graph_open_duration_seconds`
  = 显式 open 点客户端近似（cold/warm 按图名存在性）；`graph_lru_cache_hit_ratio` =
  离线代理（闲置后首请求且 Stage 高耗时占比，阈值 env 占位待标定）；`n_now_lag_seconds`
  改名 `ex_last_write_age_seconds`（DMS 顺带发 max/p95 gauge，{dimension} 标签）；
  `cell_watermark` 实现名 `lethefield_cell_watermark_ratio`（命名规则强制单位后缀）。
  ③ 收口定案形态：`lethefield_training.recall_filter` 轮询 es-ops → CALIBRATION 闸门 →
  feeds/raw（at-least-once + checkpoint state 文件）；召回明细带 event_id + stage_ms，
  worker `RecallWindow.mark_seen` 按 ID 去重（缺 event_id fail-closed）；API 训练 topic
  发布代码已整体删除。compose 新增 prometheus(9090)/grafana(3000)（scrape 走
  host.docker.internal——服务在宿主机 uv run；Grafana provisioning datasource + 三线六面板）。
- M13 定案（六条红线工程化，升级确认已入档开发文档 §14）：红线 1 =
  `scripts/check_space_filter.py`（AST 三规则：图遍历字符串必须同串 `has('space_id'`
  ——纯 `.V().count()`/`.E().count()` 豁免（per-space 图计数，图名即 space）；跨
  space/集群级调用（getGraphNames/size_estimates/indices.stats/list_spaces 系/lethefield-logs
  读取）所在文件须带 `@redline1_exempt(worker=, reason=, cadence=)` 登记（装饰器单点
  `libs/clients/redline.py`，**扫描器只认登记不认注释**）或进内置豁免表；含 argparse 的
  入口必须有 `--space(s)`/`space_id` 收敛口或豁免）。红线 2 = `lethefield_rms.quota`
  （QuotaConfig 顶点/边/向量上限占位 1M/5M/1M 待标定；**图 count 短 TTL 缓存 = 近似执行、
  超发有界**；writer 三原语 + `index_vector` 默认开启强校验（M15 写入链接入即继承）；
  `QuotaExceeded` → API 429 rate_limited、message 含 quota_exceeded（不新增契约码）；
  `RetrieveConfig.max_returned_nodes` 返回节点数硬上限；ES 字节走 space_storage_bytes
  监控非配额；`quota_for_tier` 覆盖机制留着，1.0 不做 per-space）。
  **`QuotaCounters.vector_count` 必须 routing + space_id term 双机制**——只带 routing 会把
  共享分片上他 space 文档计入（M13 实测 27 条串计数致误拒），与 kNN 零泄漏同款教训。
  红线 3 = sweep 冷热分频（`lethefield_fs.config.sweep_due` 纯函数：cold 走
  `cold_interval_seconds` 占位 600s、hot/premium/未知 tier 走热节奏保障优先；tier 经
  `list_space_mappings()` 每轮取，last-swept 复用 `fs:sweep:last_ok:{space}` 心跳键）；
  **Redis 逐出豁免定案**（现存键全小键、ex:n 是权威计数非缓存、逐出会破坏 n 分配）——
  不配 maxmemory-policy，配套 = compose redis AOF（appendonly yes + everysec）+ DMS 第 4 路
  n 一致性巡检（Redis ex:n vs EX MAX(n)，回退 = 序号重复分配风险，page 级；键缺失不告警，
  n_now 重建是设计路径）。**归档快照必须携带原始 v_i**（embedding 不可重放）：
  archive_node 删 ES 文档前先 get_vector 进快照 `"v"` 键；rebuild 经 `vector_lookup`
  注入保持 replay 纯函数，执行层两级 lookup（源侧旧 archived_nodes → rms_vectors），
  缺口 emit `rebuild_vector_missing`；migrate 传 `source_cell_session` + es 走源侧 lookup。
  **共享 rms_vectors 维持定案**（per-space 向量索引 = 索引数随 space 爆炸，不留活口）。
  红线 4/5/6 = `scripts/check_redlines.py` 汇总核验（静态进 CI；红线 5 口径 = destroy 全文
  位置比对、migrate 按 `_evict_graph` 先于 `_drop_graph_storage` 调用点比对——EX scratch
  DROP 不属红线 5 语义；`--runtime` 跑 clock_monitor + check_graph_config，集成测试调起）。
  两个脚本均已接 ci.sh。
- M14 定案（SS 显著性打分服务 `services/ss`，lethefield-ss；升级确认入档开发文档 §15 +
  修订记录 19–22 条）：**契约 1 首次演进**——meta_events 加可空 `details` 列（text/JSON，
  按 meta_type 分型；scoring_result 的六维原始值+合成 s+模型版本+事件引用 schema 单点在
  `lethefield_rms.schema`；reinforce 置空行为不变；M10 migrate `_META_COLUMNS` 已同步）；
  元事件纯 INSERT 单点 `ex_n.append_meta_row`（ex_ingest 合并分支委托）。**EX→Pulsar 生产侧**：
  `services/api/stream_publisher.py` 显式登记单点（Pulsar import 只许此模块，M5 结构性断言
  按新口径）；`append_experience` 落库确认后发布 `persistent://lethefield/{space}/ex-events`
  （有限重试 3×0.2s，**失败不阻塞同步返回**——EX 是 SoT，page 事件 `ex_stream_publish_failed`
  + `lethefield_ex_stream_publish_total{result}`）；M5 红线修订为"同步返回路径不依赖 Pulsar"。
  信封/topic 名单点 `libs/clients/ex_stream.py`（ExStreamEvent/ScoringResult，版本化
  fail-closed）。**SS worker**：逐 space consumer（`list_spaces()` 枚举 + 节流刷新——
  **Pulsar 跨 namespace 正则订阅实测 InvalidTopicName，namespace 段不许通配**）；
  **死信走应用层**（runtime 按 message_id 计失败次数，超 `max_redeliver_count` → 原文写
  `ex_events_dlq_topic`（命名单点 `<topic>-ss-scorer-DLQ`）+ ack + page `ss_scoring_dlq`）——
  **不要用 broker ConsumerDeadLetterPolicy：standalone + pulsar-client 实测
  redelivery_count 恒 0、死信转移不触发**；失败 nack 不重投到死；**n 连续性自愈**：
  NTracker 冷启动从 EX 最新 scoring_result 的 n_at_event 播种，缺口 page `ss_n_gap` +
  按 n 区间从 EX 补偿；EX 回写幂等（已有 scoring_result 不重打分、从 details 重建信封补发下游）；
  run_once 里失败消息**不计 progressed**（否则毒消息重投把排空轮喂成死循环）。
  **LLM 客户端**：OpenAI 兼容 HTTP 直连（httpx，`SS_LLM_BASE_URL/API_KEY/MODEL` 从根 `.env`
  dotenv 单点加载，缺失 fail-closed；key 不进日志/指标/异常）；**降级分级**：缺 1 维 →
  中性值（`SS_DEGRADE_NEUTRAL` 默认 0.5）+ degraded + 缺失维清单随 details 落 EX +
  `lethefield_ss_score_degraded_total{dimension}`；不可解析或缺 ≥2 维 → 失败 → DLQ；
  `SS_DEGRADE_POLICY=neutral_mark|retry` 默认 neutral_mark。权重禁硬编码（`SSConfig.weights`
  默认均权，`LETHEFIELD_SS_WEIGHTS_JSON` 覆盖）；标定线指标 `lethefield_ss_score_ratio{dimension}`/
  `lethefield_ss_llm_calls_total{result}`/`lethefield_ss_llm_tokens_total{type}`；端口 9105。
  M7 重建默认切全保真档（见 M7 行）。样例集 `services/ss/samples/stability_samples.jsonl`，
  稳定性报告 `deploy/baselines/m14_ss_stability.json`，结论进决策留痕。
- M15 定案（写入链 worker `services/writer`，lethefield-writer；升级确认入档开发文档 §16 +
  修订记录第 23 条）：消费 `scoring-results`（订阅 `rms-writer`，名单点
  `ex_stream.SCORING_RESULTS_SUBSCRIPTION`）建 Event-Node。**信封只作触发 + s/node_key
  来源**：c_i/τ_i/A_i 按 n 反查 EX（`ex_n.get_experience_event` 主键点查 /
  `list_experience_events_range` 区间查，n 即主键；A_i 取 agent_actor_id 盖章列，
  禁从事件体文本读）；一致性校验 fail-closed（node_key == node_key_of(event_id)、
  信封 space == topic namespace、信封 event_id == EX 行，不符走失败路径 → DLQ）。
  **node_key 冻结 `ev_{event_id}`**（SS/writer/rebuild 三处对齐，rebuild 注释转正）。
  **幂等三分解**：顶点 `vertex_exists` / 时序边 `temporal_edge_exists` / 向量 `get_vector`
  各自预检、缺失才补写，三项全在才 duplicate（三查询原语在 rms writer.py；部分失败
  重试走补全路径）。**时序边前序 = 图内 n_created < n 的最大者**
  （`latest_event_node(before_n=)`，n_created 无索引 per-space 图内扫描，标定归 §20；
  归档缺口不跨接，理想链归 M7 重放）。**n 连续性**：NTracker 冷启动从图内 max
  n_created 播种（writer 产出口径，M7 重建后同样正确），缺口 page `writer_n_gap` +
  按 n 区间从 EX 补偿（s 取 scoring_result details 全保真；details 缺失跳过等 SS
  补偿重发，正常路径幂等兜底）。**embedding = OpenAI 兼容 /embeddings HTTP 直连**
  （httpx 零 SDK，`LETHEFIELD_EMBED_BASE_URL/API_KEY/MODEL/DIMS` 根 .env dotenv 单点，
  缺失 fail-closed；**共享 rms_vectors 必须同模型同维度**——返回维度不符按失败处理，
  模型变更 = 向量全量重建，选型与变更进决策留痕；DeepSeek 无 embeddings 端点，
  真实冒烟前需另配 provider）；worker 启动 `ensure_vectors_index(dims=embed_dims)`
  校验不符拒启动（rms_vectors 生产路径首个 owner）。应用层 DLQ 单点
  `scoring_results_dlq_topic`（`<topic>-rms-writer-DLQ`）。元事件结构性不进本链路
  （reinforce 不经 stream_publisher）。metrics 端口 **9106**。
- M16 定案（IS 简版 `services/is`，lethefield-is；升级确认入档开发文档 §17 +
  修订记录第 24 条）：CLI 优先形态（与 M17"不建 Web 后台"一致），无常驻进程无 metrics
  端口。**契约 3 首次演进**：JWT 加标准注册 claim `jti/exp/iat`（四字段结构不变，
  沿用契约 1 演进规则"只加不改"；无 jti 旧 dev token 验证侧跳过吊销检查，向后兼容）。
  **吊销 = jti 吊销列表 + 有限时效**（PG `is_credentials.status`，API 验证侧逐请求
  检查、checker 异常 fail-closed 传播不静默放行；默认 TTL 24h，
  `LETHEFIELD_IS_TOKEN_TTL_SECONDS` 覆盖；1.0 不做刷新机制，重签发即刷新）。
  **scope 白名单与 JWT 密钥解析单点在 `libs/clients/credentials`**
  （`CREDENTIAL_SCOPES`/`DEBUG_SCOPE`/`jwt_secret`，api.auth 与 is.tokens 同源引用，
  禁双拷贝）；凭证 store 同模块下沉（API 验证侧与 IS 签发侧共用，同 M11 auth_registry
  先例）。**debug scope 签发侧闸门 fail-closed**：非 `--internal` 渠道拒签。
  **签发先落吊销列表行再签名**（崩溃窗口不产出无法吊销的 token）。
  账号/归属两表只被 IS 用留在 services/is/store.py；**空间创建顺序**：校验账号
  （存在且 active）→ validate_space_id → provision 成功后才写 `is_space_owners`
  归属行（无半开通状态）。§12.4 授权注册表入口收在 IS CLI
  （`auth grant/revoke/list --space`，内部走 space_ref_of 哈希）。
- M17 定案（运维操作面 `ops/ops_cli`，lethefield-ops-cli；升级确认入档开发文档修订记录
  第 25 条）：**1.0 运维写入口唯一收口**——九条命令 `space status/destroy/set-tier`、
  `migrate rebalance/to-cell/evacuate`（迁移三类触发）、`auth revoke`、
  `cell watermark/register`，全部**必选** `--space`/`--cell` 绑定（evacuate 也是显式
  space 列表，无全局形态；静态检查 = parser 内省单测 `test_no_global_commands.py`）。
  scheduler/training/is 既有 CLI 保留为底层入口。**留痕包装单点
  `lethefield_ops_cli.audit.run_with_audit`**：PG 预检 fail-closed（留痕库不可达拒绝
  执行——处置类与留痕的原子要求由此满足）→ 执行 → `DecisionLogStore.submit`
  （操作人 `--operator` > env `LETHEFIELD_OPERATOR` > OS 用户；`outcome` 恒
  `accepted`——枚举语义是人类对建议的处置结果，执行成败记 `context.result`）；
  业务已执行但留痕写入失败 → 退出码 2 + 人工补录提示。tier 升降 =
  `MappingTableControlPlaneStore.update_space_tier`（本模块新增扩展方法，ABC 冻结
  六方法不动）；新 Cell 筹备 = `cell register`（映射行登记，endpoints 必含
  cassandra/es，幂等覆盖写；基础设施由运维自备）。配额用量展示为近似值（图计数
  TTL 缓存语义，输出注明）。
- **pytest 同名测试文件冲突**：tests 目录无 `__init__.py`，两个服务同名 `test_worker.py`
  会 import file mismatch——新服务测试文件名必须全局唯一（M14 踩坑，改 `test_scoring_worker.py`）。
- ES 排序不能用 `_id`（fielddata 限制，报 search_phase_execution_exception）——日志游标
  按 timestamp 单字段排序，同毫秒边界重读由 event_id 去重兜底（M12 实测）。
- 拉新镜像注意：colima VM 的 dockerd 代理指向宿主机 192.168.5.2:7897，若宿主机代理只监听
  127.0.0.1 会 TLS 超时——宿主机代理开 Allow LAN，或宿主机侧 crane 拉取后 docker load
  （M12 prometheus/grafana 镜像实测踩坑）。
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
| `uv run python -m lethefield_ingest_dms [--once]` | M10：EX 摄入 DMS 巡检（探针/backlog/新鲜度/n 一致性四路），page 告警退出码 1 |
| `uv run python -m lethefield_training worker [--once]` | M11：加工 worker（feed 消费 + 契约 5 销毁处置 + 热层落盘） |
| `uv run python -m lethefield_training scrub <space_ref>` | M11：撤回授权存量处置（O(清单) 定位、内容清除、骨架保留，幂等） |
| `uv run python -m lethefield_training submit-incident --problem P --diagnosis D --decision C --outcome O` | M11：② 入料口（故障/混沌案例提交，rule=R5） |
| `uv run python -m lethefield_training ex-feed --space S` | M11：④ 入料口（EX 只读派生纠错对，CONTENT_COPY 闸门，幂等） |
| `uv run python -m lethefield_decision_log submit ... [--feed]` | M11：决策留痕提交（三列已补；--feed 时 R1/R2 命中发布训练 feed） |
| `uv run python -m lethefield_training recall-filter [--once]` | M12：③ 过滤器（es-ops 召回明细 → 授权闸门 → 训练 topic，checkpoint） |
| `uv run python -m lethefield_metrics_exporter [--once]` | M12：离线聚合 worker（日志流/元数据 → 指标，:9104 暴露口） |
| `open http://localhost:9090` / `:3000`（admin/admin） | M12：Prometheus / Grafana（compose 常驻，dashboard 已 provision） |
| `uv run python scripts/check_space_filter.py` | M13：红线 1 静态扫描（无 space 过滤遍历 / 未登记跨 space 调用 / 入口缺 --space 收敛口） |
| `uv run python scripts/check_redlines.py [--runtime]` | M13：红线 4/5/6 汇总核验 + Redis 豁免记录（--runtime 需全栈，集成测试调起） |
| `uv run python -m lethefield_ss worker [--once]` | M14：SS 打分 worker（ex-events consumer → 六维打分 → EX 回写 + scoring-results） |
| `uv run python -m lethefield_ss smoke` | M14 任务二：真实 LLM 小批量冒烟（读根 .env，端点/模型/六维解析验证） |
| `uv run python -m lethefield_ss validate --samples F --out R` | M14 任务三：打分稳定性验证（分布/塌缩/成本报告 JSON，结论进决策留痕） |
| `uv run python -m lethefield_writer worker [--once]` | M15：写入链 worker（scoring-results consumer → 图顶点 + 时序边 + 向量，:9106 暴露口） |
| `uv run python -m lethefield_is account create/list/disable` | M16：账号 CRUD |
| `uv run python -m lethefield_is space create <space_id> --account A [--tier T]` | M16：空间创建入口（调 M9/M10 开通流水线，成功后登记归属） |
| `uv run python -m lethefield_is credential issue/revoke/list` | M16：凭证签发（--internal 才可授 debug）与吊销（吊销列表立即生效） |
| `uv run python -m lethefield_is auth grant/revoke/list --space S` | M16：训练数据授权注册表入口（§12.4） |
| `uv run python -m lethefield_ops_cli space status --space S` | M17：space 状态查询（映射/tier/水位/配额用量，自动留痕） |
| `uv run python -m lethefield_ops_cli space destroy --space S [--reason T]` | M17：整 space 销毁处置（M9/M10 流水线 + 契约 5 广播） |
| `uv run python -m lethefield_ops_cli space set-tier --space S --tier T` | M17：tier 升降调整 |
| `uv run python -m lethefield_ops_cli migrate rebalance/to-cell/evacuate ...` | M17：迁移三类触发（再平衡 / 指定目标 / Cell 退役，均显式绑定） |
| `uv run python -m lethefield_ops_cli auth revoke --space S` | M17：授权撤回处置（注册表撤回 + 热层 scrub） |
| `uv run python -m lethefield_ops_cli cell watermark --cell C [--refresh]` / `cell register --cell-id C --endpoint k=v` | M17：Cell 水位查看 / 新 Cell 筹备触发 |
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
