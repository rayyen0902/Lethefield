"""RMS 图 Schema 与写入链地基（M2，开发文档 §3）。

- schema：节点/边 schema 常量与幂等初始化（ensure_graph_schema）
- vectors：rms_vectors 独立向量索引（node_key 关联，space_id routing）
- writer：写入链地基原语（M15 在此之上组装）

子模块按需导入（from lethefield_rms.schema import ...），包级不做 eager 汇出，
避免 python -m lethefield_rms.schema 的 runpy 双重导入告警。
"""
