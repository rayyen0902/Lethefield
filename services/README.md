# 服务 = 进程边界（1.0 开发范围）
#
# 本目录放置各服务的独立可部署单元。M0 阶段不写任何业务代码（开发文档 M0「明确不做」），
# 后续模块按开发文档认领：
#
#   services/is/    M16 IS 简版（账号/凭证/JWT）
#   services/ex/    M10 EX 摄入与事件存储
#   services/ss/    M14 SS 六维度显著性打分
#   services/rms/   M2/M3/M4/M7 RMS 图 schema / FF / 检索 / 纠错
#   services/writer/  M15 写入链 worker（scoring-results → 图顶点 + 时序边 + 向量）
#   services/fs/    M6 FS sweep worker
#   services/api/   M5 MCP / SDK 接口层
#   services/scheduler/  M9 租户调度器（space→Cell 映射 / 开通 / 注销 / 水位）
#
# 共享代码只允许放 libs/ 三样（日志 schema、指标 registry、存储/Pulsar 客户端），
# 禁止在服务间共享其他代码（开发文档 M0 任务 2）。
