"""FS sweep worker（M6，开发文档 §7）。

职责边界（不可扩大）：只做 sweep 三件事——忽视惩罚执行、n_star_cached 刷新、
归档/固化判定与执行。不做任何图拓扑推断（实体/因果边推断归 consolidation worker）。
"""
