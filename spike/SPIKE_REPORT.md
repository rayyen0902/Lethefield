# Lethefield 技术栈 Spike 验证报告

**时间**：2026-07-29 ｜ **环境**：macOS（16GB/8核）+ colima Docker（4C/8G），Cassandra 4.1 / Elasticsearch 8.13.4 / JanusGraph 1.0.0 单节点
**对应设计文档**：Lethefield-RMS-设计文档 v1.0（§4、§5、§7、§15、§10 课题 1）
**脚本**：`scripts/q1_graph_rw.py`、`q2_vector_index.py`、`q2a_recover.py`、`q3_graph_open.py`、`q4_business_loop.py`（均可重复执行）

---

## Q1：单节点起栈 + 图读写 —— PASS

**结论**：JanusGraph 1.0.0 + Cassandra + ES 单节点栈正常起服务，图读写、1-2 跳遍历、bytecode API 均正常。

**证据**：gremlin server 就绪 16.2s（会话内测量）；写入 3 顶点 2 边并读回；`alice -2hop knows-> carol` 遍历正确；bytecode 计数 3。

---

## Q2：dense_vector kNN 与 JanusGraph mixed index 的协同 —— PASS（两条路径均可行，推荐独立向量索引）

**结论**：
- **Q2a mixed index 全文检索可行**。注意两个实测事实（与文档/教科书有出入）：
  1. JanusGraph 1.0.0 已**移除** `org.janusgraph.core.Vertex` / `org.janusgraph.core.attribute.Text`（jar 内核实不存在）；建索引用 TinkerPop 的 `org.apache.tinkerpop.gremlin.structure.Vertex`，全文谓词用 **TextP**（`TextP.containing`，TinkerPop 3.7 引入）。
  2. JanusGraph 为每个 mixed index 建**独立 ES 索引**（实测名为 `janusgraph_contentbytext`），单分片 1 副本；新索引状态停在 REGISTERED 即可用（未自动转 ENABLED）。
  3. 一次运维教训：索引状态卡 INSTALLED 时 `ENABLE_INDEX` 报 IllegalArgumentException，DROP 重建 + 重启实例后恢复——单实例 schema 注册是脆弱点，值得进运维 runbook。
- **Q2b 同索引加 dense_vector 可行**：可直接对 `janusgraph_contentbytext` PUT `_mapping` 加 `dense_vector(384, cosine)` + `space_id`，kNN 正常返回（c0 簇得分 0.83/0.83 显著高于其他簇 0.53/0.49）。
- **Q2c 独立向量索引 + custom routing 可行（推荐）**：`rms_vectors`（2 分片 0 副本），`_search_shards?routing=spaceA` 确认**单分片命中**；带 routing 的 kNN 仅返回本 space 文档，无跨 space 泄漏（断言通过）。

**实现路径建议**：采用 **(c) 独立向量索引 + 直连 ES**（不经 JanusGraph 索引层），与 §5"Stage 2 由 ES 承担"一致；(b) 证明同索引共存可行，可作为备选。§4.1 `v_i` 的"与该节点全文/属性字段同索引存放"实现上应为"同集群、独立向量索引"。

---

## Q3：graph open 延迟实测 —— PASS

**结论**：动态图（ConfiguredGraphFactory + 每图独立 keyspace）的创建/打开开销在**秒级**，温启动亚秒，热查询毫秒级——§15"存储物理隔离、计算共享"的每-Space 独立图模型在单节点小规模下成立，可直接喂 §10 课题 1。

**实测数据**（n=5 个动态图，`scripts/q3_graph_open.py`，日志 `q3_output.log`）：

| 指标 | min | p50 | max |
|---|---|---|---|
| 容器重启 → gremlin 可服务 | — | 51.8s | — |
| create（建新图，keyspace+表初始化） | 832ms | 921ms | 1445ms |
| open（冷开，create 后首开） | 3017ms | 3564ms | 4898ms |
| reopen（close→open 温启动） | 398ms | 482ms | 557ms |
| 首开查询（addV+commit，含 schema 惰性创建） | 1919ms | 1942ms | 2000ms |
| 后续查询（count，热） | 24ms | 48ms | 114ms |

- 每图独立 Cassandra keyspace 已验证（`spikeq31`–`spikeq35` + `janusgraphconfig` 配置图）。
- `ConfiguredGraphFactory.create(name)` 模板路径不可用（NPE：需先 `createTemplateConfiguration`），实际走显式 `createConfiguration(MapConfiguration)`——产品侧 space 开通流程应直接封装显式配置路径。
- 冷开 3–5s 意味着"冷启动架构"下首个请求有明显首开延迟，需要 §15 规划的常驻/预热策略兜底；温启动 ~0.5s 可接受。

**踩坑记录（运维红线，务必进 runbook）**：
1. **`ids.authority.wait-time` 不是"等 ID 分配的超时"**，而是 ID block 认领后的竞争确认睡眠（源码 `ConsistentKeyIDAuthority.getIDBlock`：写完认领后睡 wait-time×1.1 再回读确认）。误调大到 120s 会导致每次 ID 申请固定睡 132s 而等待方先超时，写入 100% 失败（`StandardIDPool.waitForIDBlockGetter` TimeoutException）。**保持默认（100ms 量级），禁止调大**。
2. **禁止在 JanusGraph 实例在线时 DROP 其使用的 keyspace**——会使服务端 prepared statement 引用失效表 ID（`Unknown CF`）并污染后续重建。注销流水线必须先停/隔离实例再删存储。
3. colima/虚拟化环境的**时钟跳变**（本次 VM 时钟一度快 68 分钟后回跳）会让 Cassandra LWW 写入静默失效并挂起 ID 分配；生产部署必须保证节点时钟同步（NTS/NTP 硬化）。

---

## Q4：最小业务闭环 —— PASS

**结论**：EX 事件写入 → RMS 顶点+时序边 → ES 向量（routing=space_id）→ retrieve（kNN 锚点 → 1 跳子图 → FF 现算 θ 硬过滤）全链路跑通，四项断言全过：

1. **高分存活节点召回**：e17/e18/e19（s=0.9、Δn 小、s_eff≈0.72+）被正确召回并保留；
2. **低 s 过滤**：e5（s=0.2）被 θ=0.3 过滤；
3. **FF 衰减真实生效**：e7（s=0.9 但 n_last_touched=0、Δn=20，s_eff=0.1000）被过滤——证明"检索时现算衰减"而非"写入时定死"的机制可行；
4. **跨 space 隔离**：spaceA/spaceB 各自 20 事件，retrieve 结果 100% 属于本 space（ES routing + 图内 space_id 双重验证）。

**链路形态**（对应 §5）：ES kNN（k=5，routing 单分片）→ 锚点 node_key → gremlin `both('next')` 1 跳子图（15 节点）→ Python 侧现算 `s_effective = s×e^(−λ·Δn·log(1+t/t₀))`（λ=0.1，t₀=3600，t=7200）→ θ 硬过滤（kept 7 / dropped 8）。

**一处设计自检收获**：断言初稿假设"高分节点 e0 被召回"，实测 e0 因 n_last_touched=0 衰减到 0.10 被过滤——FF 语义下"高分"必须同时"近期被触碰"，这正合 §5 的设计意图（s 是上限，时效性由衰减保证），文档无需改，但后续写 API 示例时要注意这个直觉陷阱。

---

## 对设计文档的影响（汇总）

1. §4.1 v_i 实现注记：向量存独立 ES 索引（如 `rms_vectors`），经 `node_key` 与图顶点关联，kNN 直连 ES 带 routing——设计决策不变，实现路径明确。
2. 实现层注意：JanusGraph 1.0 全文谓词为 TextP（非旧 Text.*）；建索引元素类为 TinkerPop Vertex。
3. Q3 数字直接喂 §10 课题 1（graph open 首开延迟）与 §15 四层模型的"秒级首开"假设验证：**冷开 p50≈3.6s、温开 p50≈0.48s、热查询 p50≈48ms**——"冷 Space 按需拉实例"架构下，热 Space 常驻或实例预热是必要的。
4. Q4 证明 retrieve 链路（ES 锚点 + 图子图 + 检索时现算衰减）端到端可行，FF 不需要写路径预计算衰减值。
5. 运维红线（Q3 踩坑）：`ids.authority.wait-time` 保持默认；keyspace 删除必须先隔离实例；节点时钟同步是硬性前提。
