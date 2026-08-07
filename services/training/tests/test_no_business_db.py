"""验收硬性项：加工 worker 不查业务库（红线 1）——静态扫描强制。

worker 侧模块只允许 topic / 本地热层 / 授权注册表（PG 元数据）路径；
任何 gremlin / cassandra / ex_n（EX 实时库读层）import 都是违规。
（ex_feed 是生产侧 EX 只读入料口，不在 worker 侧约束内。）
"""

import ast
from pathlib import Path

import lethefield_training

WORKER_SIDE_MODULES = ["worker", "hot_store", "recall_window", "sample", "config"]
FORBIDDEN_ROOTS = {"gremlin_python", "cassandra", "elasticsearch"}
FORBIDDEN_EXACT = {"lethefield_clients.ex_n"}


def _imports_of(module_name: str) -> set[str]:
    path = Path(lethefield_training.__file__).parent / f"{module_name}.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_worker_side_modules_do_not_touch_business_stores():
    for module in WORKER_SIDE_MODULES:
        imports = _imports_of(module)
        roots = {name.split(".")[0] for name in imports}
        assert not (roots & FORBIDDEN_ROOTS), f"{module} 引入业务库存取：{roots & FORBIDDEN_ROOTS}"
        assert not (imports & FORBIDDEN_EXACT), (
            f"{module} 引入 EX 读层：{imports & FORBIDDEN_EXACT}"
        )
