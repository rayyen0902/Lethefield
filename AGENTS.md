# AGENTS.md

## 项目阶段

M0–M6（工程地基 / 存储基础设施 / RMS 图 Schema / FF 引擎 / 检索流程 / MCP·SDK 接口层 /
FS sweep worker）已完成并验证（M0–M6 CI 全绿）。
**下一个模块：M7 纠错机制（supersedes）**（开发文档 §8：纠错事件化、同一对节点幂等、
reinforce 时间窗合并；重放重建脚本须复用 `ff.archive_eligible` 与 libs/clients archive 访问层）。
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
- compose 里 Cassandra 必须显式设 `CASSANDRA_BROADCAST_RPC_ADDRESS`（官方镜像重启后会失效）。
- 测试图只 close 不 DROP 会累积 keyspace/table 拖垮 Cassandra schema 操作（实测 51 keyspace/463 表时
  图创建连锁超时）——套件莫名变慢/超时先 `make reset` 再怀疑代码。
- bind mount 内容变更不触发 compose recreate，`docker compose restart <svc>` 才重读配置；
  JanusGraph 已配 `graph.replace-instance-if-exists=true` 与 `evaluationTimeout: 180000`
  （容器重建撞实例注册冲突 / 冷图创建在全量负载下超默认 30s，两处实测）。
- spike 遗留容器（spike-elasticsearch 等）已停止但未删除，端口 8182/9042/9200 若被占先检查它们。

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

## 约定

- Python 统一（uv workspace，>=3.12）；服务 = 进程边界，共享代码只允许 `libs/` 三样。
- 所有存储访问必须经 `libs/clients` 的 `ControlPlaneStore` 抽象，禁止绕行直连。
- 指标命名 `lethefield_<域>_<名称>_<单位>`；标签白/黑名单由 `libs/metrics` 强制，
  `space_id` / `node_key` 永不可作聚合指标标签（space 粒度明细走 `libs/logschema` 日志事件）。
- `tests/integration` 是 spike q1–q4 的 CI 基线：高分召回 / 低分过滤 / 衰减过滤 / 跨 space 隔离。
  改动检索、图 schema、向量索引相关代码时必须保持其四断言全绿。
- 红线（详见设计文档 §11.5）：禁止跨 space 全局扫描；`ids.authority.wait-time` 保持默认；
  禁止在线 DROP JanusGraph 使用的 keyspace；节点时钟同步是硬性前提。
- 不执行 git commit / push 等 git 变更操作，除非用户明确要求。
