"""RMS 图 schema 定义与初始化（开发文档 §3，M2）。

schema 常量在此单点定义，ensure_schema.groovy 脚本只含逻辑、元素名经绑定传入，
两侧一致性由 services/rms/tests 与 scripts/check_rms_schema.py 强制。
"""

import argparse
from importlib import resources

from gremlin_python.driver.client import Client
from lethefield_clients import gremlin_client

# 顶点属性键 → JanusGraph 类型简单名（对齐开发文档 §3 节点表，共 17 个，M6 增 consolidated_at）
EXPECTED_PROPERTY_KEYS: dict[str, str] = {
    "node_key": "String",  # 顶点全局唯一标识（唯一复合索引 byNodeKey）
    "space_id": "String",  # 图遍历一律 has("space_id", sid) 开头（红线：禁全局扫描）
    "node_type": "String",  # "event" / "entity"
    "content": "String",  # c_i 事件内容
    "tau": "Date",  # τ_i 时间戳
    "agent_actor_id": "String",  # A_i 一等属性（设计文档 §8）
    "attrs": "String",  # A_i 其余结构化属性的 JSON 序列化
    "ref_ex": "String",  # 指回 EX 原始事件 ID，RMS↔EX 唯一关联机制
    "s": "Double",  # φ_i.s 基准显著性（仅 δ 调整写回）
    "n_created": "Long",  # φ_i 创建时的事件序号
    "n_last_touched": "Long",  # φ_i 最近一次被强化/冲突失效时的事件序号
    "n_star_cached": "Long",  # φ_i 缓存的遗忘视界预测值
    "reinforce_count": "Integer",  # φ_i 累积强化次数
    "conflict_count": "Integer",  # φ_i 累积冲突失效次数
    "neglect_count": "Integer",  # φ_i 累积忽视惩罚次数（M6 用）
    "consolidated_at": "Date",  # 固化时间戳（M6 第 17 键定案）：存在即固化态，s 锁定
    "entity_key": "String",  # 实体顶点标识（非唯一复合索引 byEntityKey）
}

# 四类关系图 + 纠错边；temporal immutable，所有边均不参与衰减（方案 A：衰减只作用于节点）
EDGE_LABELS: tuple[str, ...] = ("temporal", "semantic", "causal", "entity", "supersedes")

# M7 红线（设计文档 §13.7）：schema 禁止任何"硬失效标志"字段——supersedes 边记录事实，
# "是否返回"下沉检索策略。tombstone/invalidated 类命名出现在属性键/边标签即违规。
FORBIDDEN_FLAG_TOKENS: tuple[str, ...] = (
    "tombstone",
    "invalid",
    "deleted",
    "superseded",
    "expired",
    "disabled",
    "is_valid",
    "is_active",
)


def find_invalidation_flags(names: list[str]) -> list[str]:
    """扫描命名中的失效标志禁项（子串匹配，大小写不敏感），返回违规名列表。"""
    return [name for name in names if any(token in name.lower() for token in FORBIDDEN_FLAG_TOKENS)]


# (索引名, 属性键名, 是否唯一)；默认 multiplicity，边不加约束
COMPOSITE_INDEXES: tuple[tuple[str, str, bool], ...] = (
    ("byNodeKey", "node_key", True),
    ("byEntityKey", "entity_key", False),
)

# 图后端默认指向 compose 内的 cell Cassandra + 图索引 ES（服务名，JG 容器内可达）；
# M9 起调度器开通时按映射表 Cell endpoints 推导覆盖（backend_props 参数）。
GRAPH_BACKEND_PROPS: dict[str, str] = {
    "storage.backend": "cql",
    "storage.hostname": "cassandra-cell",
    "index.search.backend": "elasticsearch",
    "index.search.hostname": "es-graph",
    "cache.db-cache": "false",
}


def backend_props_of(endpoints: dict[str, str]) -> dict[str, str]:
    """从 Cell endpoints（JanusGraph 容器视角）推导建图配置（M9，调度器用）。"""
    return {
        "storage.backend": "cql",
        "storage.hostname": endpoints["cassandra"],
        "index.search.backend": "elasticsearch",
        "index.search.hostname": endpoints["es"],
        "cache.db-cache": "false",
    }


def ensure_schema_script() -> str:
    """读取 ensure_schema.groovy 资源文本。"""
    return resources.files("lethefield_rms").joinpath("ensure_schema.groovy").read_text()


def ensure_graph_schema(
    client: Client, gname: str, backend_props: dict[str, str] | None = None
) -> None:
    """对图 gname 幂等落地全量 RMS schema（图不存在则按 backend_props 建图，
    缺省 GRAPH_BACKEND_PROPS；M9 起调度器按 Cell endpoints 传入）。"""
    result = (
        client.submit(
            ensure_schema_script(),
            {
                "gname": gname,
                "backendProps": backend_props or GRAPH_BACKEND_PROPS,
                "keySpecs": [[n, t] for n, t in EXPECTED_PROPERTY_KEYS.items()],
                "edgeNames": list(EDGE_LABELS),
                "indexSpecs": [[name, key, unique] for name, key, unique in COMPOSITE_INDEXES],
            },
        )
        .all()
        .result()
    )
    if "ok" not in result:
        raise RuntimeError(f"图 {gname} schema 初始化未返回 ok：{result}")


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化/补齐某 space 图的 RMS schema（M2，幂等）")
    parser.add_argument("gname", help="图名（= keyspace 名，每 space 一个图）")
    args = parser.parse_args()

    client = gremlin_client()
    try:
        ensure_graph_schema(client, args.gname)
    finally:
        client.close()
    print(f"[ok] 图 {args.gname} RMS schema 已就绪")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
