"""M17 静态检查（开发文档 §18 验收第 2 条）：CLI 中不存在无 space/cell 绑定的
全局操作命令——每条叶子命令必须带**必选**的 space/cell 绑定参数（红线 1 操作面落实）。

parser 内省实现，随 make test / ci.sh 运行，不依赖起栈。
"""

import argparse

from lethefield_ops_cli.__main__ import build_parser

# 触发点覆盖清单（§18 命令清单）：缺一条即验收不过
EXPECTED_COMMANDS = {
    ("space", "status"),
    ("space", "destroy"),
    ("space", "set-tier"),
    ("migrate", "rebalance"),
    ("migrate", "to-cell"),
    ("migrate", "evacuate"),
    ("auth", "revoke"),
    ("cell", "watermark"),
    ("cell", "register"),
}

_BINDING_OPTIONS = {"--space", "--spaces", "--cell", "--cell-id"}
_BINDING_DESTS = {"space", "spaces", "space_id", "cell", "cell_id"}


_Leaf = tuple[tuple[str, ...], argparse.ArgumentParser]


def _leaf_parsers(parser: argparse.ArgumentParser) -> list[_Leaf]:
    leaves: list[_Leaf] = []

    def walk(p: argparse.ArgumentParser, trail: tuple[str, ...]) -> None:
        subs = [a for a in p._actions if isinstance(a, argparse._SubParsersAction)]
        if not subs:
            leaves.append((trail, p))
            return
        for sub in subs:
            for name, child in sub.choices.items():
                walk(child, trail + (name,))

    walk(parser, ())
    return leaves


def _binding_args(parser: argparse.ArgumentParser) -> list[argparse.Action]:
    store_like = (argparse._StoreAction, argparse._AppendAction)
    return [
        a
        for a in parser._actions
        if isinstance(a, store_like)
        and (set(a.option_strings) & _BINDING_OPTIONS or a.dest in _BINDING_DESTS)
    ]


def test_trigger_point_coverage():
    """§18 命令清单逐条存在（space 状态/迁移三类/销毁/撤回/tier/水位+新 Cell 筹备）。"""
    leaves = _leaf_parsers(build_parser())
    assert {trail for trail, _ in leaves} == EXPECTED_COMMANDS


def test_every_leaf_command_binds_space_or_cell():
    """每条叶子命令都有**必选**的 space/cell 绑定参数——无"对全部执行"的全局形态。"""
    for trail, parser in _leaf_parsers(build_parser()):
        bindings = _binding_args(parser)
        assert bindings, f"命令 {' '.join(trail)} 缺少 space/cell 绑定参数"
        optional = [a for a in bindings if not a.required]
        assert not optional, (
            f"命令 {' '.join(trail)} 的绑定参数 {[a.dest for a in optional]} 不是必选"
        )
