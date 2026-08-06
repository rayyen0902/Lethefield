"""租户调度器（M9 Cell 架构 + 水位制调度，开发文档 §10 / 设计文档 §17）。

无状态控制面：space→Cell 映射的开通 / 注销 / 水位调度，元数据持久化于
`lethefield_control` keyspace（MappingTableControlPlaneStore）。存量读写不经本
服务——计算侧持 MappingCache 直连 Cell（设计文档 §17.2 硬性验收）。

明确不做（设计已否决）：自动再平衡、动态打分装箱、跨 Cell 迁移（M10 验收项）。
"""
