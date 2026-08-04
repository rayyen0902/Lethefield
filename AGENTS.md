# AGENTS.md

## 项目阶段

M0 工程地基（1.0 开发起点）。一切设计结论以《Lethefield-设计文档》v1.7 为准，
开发执行以《Lethefield-开发文档》v1.2 为准；设计未覆盖的分支先升级确认，不自行拍板。

## 常用命令

| 命令 | 作用 |
|---|---|
| `bash scripts/ci.sh` | 本地 CI 全流程（lint + 单测 + 起栈 + 集成测试） |
| `make lint` | ruff check + format check |
| `make test` | 单元测试（libs + ops，不需要全栈） |
| `make up` / `make down` / `make reset` | 起栈 / 停栈 / 清卷重起 |
| `make itest` | 集成测试（需全栈已就绪） |

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
