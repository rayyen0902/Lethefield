# Lethefield — 课题：EX 存储与 Pulsar 在 Cell 架构下的归属

**版本**: v0.1（提案，未定稿）
**状态**: 纸面论证稿——§17.6 遗留问题、§10 立项课题的产出，供评审；通过后以新版本号并入主文档
**关联**: Lethefield-RMS-设计文档 v1.2（§0.5、§7.3、§7.5.2、§13.4、§14、§15、§17）
**日期**: 2026-07-29

---

## 1. 问题定义

§17 Cell 架构解决了 RMS 侧（图存储 Cassandra + 索引/向量 ES）的池化与调度。但一个 space 的完整数据足迹有四类存储：

| 存储 | 内容 | 现有决策 |
|---|---|---|
| RMS 图 | JanusGraph keyspace | §15.1 per-space keyspace → §17 归属 Cell |
| RMS 索引/向量 | ES index | §15.1 per-space index → §17 归属 Cell |
| **EX 事件存储** | Cassandra（不可变 source of truth） | v0.6/§7.3：与 JG 的 Cassandra **物理隔离、独立集群**——Cell 架构下归属未定 |
| **事件流** | Pulsar namespace | v0.7/§7.5.2：每 space 一个 namespace——Cell 架构下集群归属未定 |

本课题回答：**EX 存储与 Pulsar 是否随 Cell 分片？以何种粒度？** 约束条件与课题 2 相同（§0.5 公理、§13.4 销毁、§14 可携带性、服务商独立部署），另加两条本题特有约束：

1. **v0.6 的三条依据仍然有效**（§7.3）：JG 私有序列化与 EX 业务表共存冲突；EX 高频追加写与图读写混合的资源争抢；EX 可用性须独立于图负载
2. **EX 是重建链的源头**：RMS 可从 EX 完整重放重建（§13.7 可重建性）——这要求 **EX 的可用性/耐久性等级必须高于 RMS**。若 EX 与 RMS 同集群，集群故障会同时摧毁数据和它的重建源，可重建性在最关键的时刻失效

## 2. 决策 1：EX 事件存储 → 独立 EX 集群池，per-space keyspace，与 Cell 平行分片

**EX 不并入 Cell 的 Cassandra，也不退回"单一大集群 + space_id 逻辑分区"，而是：每 space 一个独立 EX keyspace，存放在与 Cell 池平行的 EX 集群池中，由同一租户调度器统一登记与水位调度。**

### 2.1 选项对比

| 方案 | 判断 |
|---|---|
| **EX 集群池 + per-space keyspace（采用）** | 延续 v0.6 隔离精神并延伸到记忆本体；销毁 = DROP KEYSPACE（§13.4 干净落地）；迁移/导出 = keyspace 快照（与 §17.2 流水线同构）；结构性保证覆盖 EX（公理 1/3） |
| EX 并入 Cell 的 Cassandra（否决） | 直接违反 v0.6 三条依据；致命伤是**集群故障同时切断数据与重建源**——RMS 可从 EX 重建的前提是 EX 活着，共集群使可重建性在最需要时失效；唯一收益是省一套集群，不符合公理 2（记忆比算力贵） |
| 单一大 EX 集群 + space_id 逻辑分区（否决） | 回避了 keyspace 上限，但引入三个更坏的问题：整 space 销毁退化为 range delete（tombstone 风暴，且不可校验"无残留"）；导出需全表扫描过滤；结构性保证在记忆本体上开倒车——公理 3 不允许 |

### 2.2 代价诚实记账

EX 集群池与 Cell 池受同一个 1000~2000 keyspace/集群上限约束，**Cassandra 集群总数约为仅 RMS 侧的两倍**（每 1000~1500 space：1 Cell + 1 EX 集群）。缓解因素：

- EX 是追加写、低频读——EX 集群可用高密度存储、低规格机型，节点成本显著低于 Cell 的 JG-Cassandra
- **封存层 EX 先进对象存储**：冷 space 的 EX keyspace 是第一批被封存的对象（§15.2 封存层），活跃 EX 集群只承载热/温 space，实际集群数小于空间总数对应的理论值
- §15.3 的 0.05~0.5 美元/space/月成本边界**需把 EX 侧计入**，本稿判断仍在该区间（靠上述两点压向低位），但必须在定价对齐时合并测算，不能只看 RMS 侧

### 2.3 调度方式

复用 §17 调度器，零新增机制：映射表 `space_id → {cell_id, ex_cluster_id, ...}`；EX 集群上报同样的水位维度（keyspace 数为主），三档状态与"不做自动再平衡"原则原样适用。开通 space 时**先建 EX keyspace 再建 RMS 存储**（source of truth 先行，失败回滚顺序反之）。

## 3. 决策 2：Pulsar → 全局集群池，不随 Cell 分片

**Pulsar 保持全局集群（池），每 space 一个 namespace 不变；不以 Cell 为粒度分片。触及上限时按粗粒度演进（一个 Pulsar 集群服务 K 个 Cell），而非对齐 Cell。**

### 3.1 论证

- **无数量硬上限压力**：namespace 是轻量元数据操作（v0.7 选型依据本身），不存在 keyspace/index 式的 1000~2000 硬约束；单个 Pulsar 集群承载百万级 namespace 没有架构性障碍
- **per-Cell Pulsar 成本直接否决**：一套 Pulsar = broker + BookKeeper + 元数据存储（ZooKeeper 或等效），最小可用规模约 7 节点——每个 Cell 配一套，消息层成本超过存储层本身，荒谬
- **故障影响面分析不支持分片**：Pulsar 宕机 = 摄入暂停，≠ 数据丢失——写入路径是同步确认的（§9.3 record 等 EX 落库确认；§7.5.1 Dead Man's Switch 监控摄入停滞），生产者失败即知、可重试。全局宕机（全部 space 摄入暂停）与 Cell 级宕机（部分 space 摄入暂停）的差异是真实的，但不值得为缩小这个差异给每个 Cell 配一套 7 节点消息集群
- **隔离需求已被 namespace 满足**：v0.7 选 Pulsar 的核心理由就是 namespace 级配额/策略隔离（存储配额、TTL、限流），消息层的"噪声邻居"防护在逻辑层已成立

### 3.2 演进路径

当单集群触及实际瓶颈（namespace 元数据、backlog 存储、跨地域部署）时，演进为**粗粒度 Pulsar 池**：一个 Pulsar 集群服务 K 个 Cell（K 由实测标定），调度器登记 `space_id → pulsar_cluster_id`。此时才出现 Pulsar 侧的迁移问题（见 §4）。

### 3.3 与 EX 的对比为什么结论不同

EX 和 Pulsar 都"不是 RMS"，为什么 EX 分片、Pulsar 不分？三个决定性差异：EX 有 keyspace 硬上限、Pulsar 没有；EX 是永久资产（公理 1 直接适用）、Pulsar 是传输管道（消息消费后价值即转移到 EX）；EX 宕机阻断重建链、Pulsar 宕机只是暂停摄入。**持久资产按资产归属分片，传输管道按吞吐供给池化**——这是两条决策背后的统一原则。

## 4. 对 §17 流水线的修订

space 生命周期流程从"双存储"扩为"三存储 + namespace"（Redis 缓存清理不变）：

- **开通**：建 EX keyspace（EX 池）→ 建 Pulsar namespace（全局池）→ 建 RMS keyspace + ES index（Cell）→ 注册映射 `{cell_id, ex_cluster_id, pulsar_cluster_id}`。source of truth 先行
- **迁移**：EX 与 RMS 各自走快照/导入流水线（同 §17.2，可并行）；**Pulsar 侧新增在途事件处理**：标记 migrating → 暂停摄入（生产者收到明确错误并重试缓冲）→ consumer 排空源 namespace backlog → 切映射 → 目标侧恢复摄入 → 源 namespace 宽限期后清理。只读窗口由三者中最慢者决定，目标仍 <1 分钟（待演练验证，EX 通常是最大头）
- **注销**：标记 destroying → 驱逐计算实例（红线 5）→ **先 RMS 后 EX**（DROP RMS keyspace + DELETE index → 删 Pulsar namespace → 最后 DROP EX keyspace——记忆本体最后消失，任何中途失败都不至于"图没了、源也没了"）→ 清缓存与映射，全链路校验无残留

## 5. 规模演算修订

每 1000~1500 space 的标准供给：**1 Cell（3 Cass + 3 ES）+ 1 EX 集群（3 节点，可低规格高密度）+ 共享 Pulsar 池的份额**。Pulsar 池按总吞吐/总 backlog 扩容，与 space 数弱相关（与活跃事件流强相关）——10 万 space 约 70~100 Cell + 同量级 EX 集群 + 1~3 套 Pulsar 集群。结论不变：瓶颈在存储供给（钱），调度器复杂度不增。

## 6. 一致性检查

- **§7.3（v0.6）**：EX 与 JG-Cassandra 物理隔离在目标架构下延续为"EX 池与 Cell 池平行"——决策精神不变，形态升级
- **§13.4 整 space 销毁**：EX 侧获得与 RMS 同构的 DROP 级销毁能力，注销顺序"先派生物后本体"为新补强
- **§14 可携带性**：EX 导出 = keyspace 快照（与 RMS 同构流水线），跨服务商迁移的三存储打包导出即整 space 完整资产
- **§0.5**：公理 1（记忆神圣）→ 结构性保证覆盖记忆本体；公理 2（记忆比算力贵）→ 接受集群数翻倍；公理 3（威胁不来自开发者）→ 排除逻辑分区方案在 EX 上开倒车
- **§7.5.2（v0.7）**：namespace 模型不变，全局池不改变任何选型依据
- **§17**：复用调度器、水位制、"不做自动再平衡"全部原则，仅映射表扩字段——Cell 架构无需修订

## 7. 待实测 / 待决项

1. **EX 集群密度实测**：append-only、低频读的负载形态下，单 EX 集群的 keyspace 承载上限是否可突破 2000（memtable 压力与活跃表相关，冷 keyspace 占比高时上限可能上移）——直接影响 EX 池成本
2. **封存层 EX 先行策略**：冷 space 的 EX keyspace 快照至对象存储的阈值与恢复流程（与 §15.2 封存层定义对齐细化）
3. **Pulsar 粗粒度分片阈值**：namespace 数 / backlog 量 / 跨地域三个触发维度，待真实负载标定
4. **迁移演练**：三存储并行迁移的只读窗口实测（EX 体量通常为瓶颈）
5. **成本合并测算**：EX 侧计入后的单 space 月成本终值，与信托侧定价对齐（§15.3 成本边界的合并口径）

---

*本稿为课题论证产出，评审通过后以新版本号并入主文档（§17.6 遗留问题随之勾除）。*
