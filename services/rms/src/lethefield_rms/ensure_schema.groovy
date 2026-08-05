// M2 RMS 图 schema（开发文档 §3）——幂等初始化脚本。
//
// 每 space 一个独立图/keyspace（keyspace 名 = 图名）；图不存在时按 backendProps 建图。
// 全部元素先查后建，重复执行不产生变更（mgmt.rollback 空事务）。
//
// 红线与设计约束：
// - 禁止在此显式配置任何 ID 分配/权威相关参数（红线 4，保持 JanusGraph 默认值，
//   参数名见设计文档 §11.5，本脚本刻意不引用以免被误判为配置项）。
// - 时序边（temporal）immutable，不参与任何衰减/sweep；衰减只作用于节点 s（方案 A）。
// - 顶点属性 schema 对齐开发文档 §3 节点表：c_i→content、τ_i→tau、A_i→attrs+agent_actor_id、
//   φ_i 状态块→s / n_created / n_last_touched / n_star_cached / reinforce_count /
//   conflict_count / neglect_count；ref_ex 是 RMS 与 EX 之间唯一关联机制。
//
// property-keys: node_key=String, space_id=String, node_type=String, content=String,
//   tau=Date, agent_actor_id=String, attrs=String, ref_ex=String, s=Double,
//   n_created=Long, n_last_touched=Long, n_star_cached=Long,
//   reinforce_count=Integer, conflict_count=Integer, neglect_count=Integer,
//   consolidated_at=Date, entity_key=String
// edge-labels: temporal, semantic, causal, entity, supersedes
// indexes: byNodeKey(node_key, unique), byEntityKey(entity_key)
//
// 绑定：gname(String)、backendProps(Map)、keySpecs(List<List<String>>，[名, 类型简单名])、
// edgeNames(List<String>)、indexSpecs(List<List>，[索引名, 属性键名, unique Boolean])。

import org.janusgraph.core.ConfiguredGraphFactory
import org.apache.commons.configuration2.MapConfiguration

// Groovy map 字面量的裸标识符 key 按字符串处理，key 即类型简单名
def types = [String: String.class, Date: Date.class, Double: Double.class, Long: Long.class, Integer: Integer.class]

def g
try {
    g = ConfiguredGraphFactory.open(gname)
} catch (Exception ignored) {
    def props = backendProps + ["graph.graphname": gname, "storage.cql.keyspace": gname]
    ConfiguredGraphFactory.createConfiguration(new MapConfiguration(props))
    g = ConfiguredGraphFactory.open(gname)
}

def mgmt = g.openManagement()
def changed = false

for (keySpec in keySpecs) {
    if (mgmt.getPropertyKey(keySpec[0]) == null) {
        mgmt.makePropertyKey(keySpec[0]).dataType(types[keySpec[1]]).make()
        changed = true
    }
}

for (edgeName in edgeNames) {
    if (mgmt.getEdgeLabel(edgeName) == null) {
        mgmt.makeEdgeLabel(edgeName).make()
        changed = true
    }
}

for (indexSpec in indexSpecs) {
    if (mgmt.getGraphIndex(indexSpec[0]) == null) {
        def builder = mgmt
            .buildIndex(indexSpec[0], org.apache.tinkerpop.gremlin.structure.Vertex.class)
            .addKey(mgmt.getPropertyKey(indexSpec[1]))
        if (indexSpec[2]) {
            builder = builder.unique()
        }
        builder.buildCompositeIndex()
        changed = true
    }
}

if (changed) {
    mgmt.commit()
} else {
    mgmt.rollback()
}
'ok'
