# AGENTS.md

## 项目阶段

M0（工程地基）、M1（存储基础设施）、M2（RMS 图 Schema）已完成并验证（本地 + GitHub Actions CI 全绿）。
**下一个模块：M3 FF 计算引擎**（开发文档 §4：FF 现算 + δ 动态更新，衰减部分永不写回存储）。
一切设计结论以《Lethefield-设计文档》v1.7 为准，开发执行以《Lethefield-开发文档》v1.2 为准；
设计未覆盖的分支先升级确认，不自行拍板。

## 已验证的环境事实（不要凭印象推翻）

- JanusGraph 1.0.0 `ids.authority.wait-time` 默认值实测 **300ms**（management toString = PT0.3S）。
- kNN 跨 space 零泄漏 = `routing`（分片收拢）+ `space_id` 过滤（语义隔离）双重机制，
  只带 routing 会泄漏（分片是多 space 共享的）——M4 Stage 2 实现必须沿用，见 tests/integration/conftest.py。
- Gremlin 绑定名避开保留字（`keys` 等）；返回值在服务端按元素逐个流回，不是嵌套列表。
- M2 RMS schema 定案：16 个顶点属性键 / 5 边标签（temporal immutable）/ `byNodeKey` 唯一复合索引
  + `byEntityKey` 非唯一；向量走 ES 独立索引 `rms_vectors`（`node_key` 关联图顶点、`space_id` routing）。
  schema 常量单点定义在 `services/rms`（lethefield_rms.schema），groovy 脚本只含逻辑、元素名经绑定传入；
  `writer.py` 是 M15 写入链地基（φ 初始化：n_last_touched=n_created、三计数器 0）。
  `ref_ex` 校验目前只覆盖 RMS 侧不变量，EX 侧 join 待 M10。
- JanusGraph 顶点不支持 `v.both(Direction, label)` 直调，邻居扩展用 traversal 的 `both()` 步骤。
- compose 里 Cassandra 必须显式设 `CASSANDRA_BROADCAST_RPC_ADDRESS`（官方镜像重启后会失效）。
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
