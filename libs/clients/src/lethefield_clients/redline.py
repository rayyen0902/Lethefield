"""红线 1 豁免登记（M13 定案）：机器可读的豁免载体——扫描器只认登记，不认注释。

红线 1 禁止的是"无 space 纪律的数据面扫描"。经映射表枚举、逐 space 独立处理、
批间节流的常驻 worker（sweep / DMS / exporter 等）是正解不是违规——但必须用
本装饰器显式登记，scripts/check_space_filter.py 才放行。

豁免三要件（reason 必须说明如何满足）：
1. 枚举走 ControlPlaneStore.list_spaces()（映射表 active 集合，禁止全集群扫描）；
2. 逐 space 独立处理（不跨 space 联合查询）；
3. 批间节流（cadence 声明节奏配置）。

装饰器本身无操作（返回原函数）：价值在 AST 可识别 + 代码评审可见的登记字段，
运行期零开销、零行为变化。
"""

from collections.abc import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable)


def redline1_exempt(*, worker: str, reason: str, cadence: str) -> Callable[[F], F]:
    """登记红线 1 豁免（无操作装饰器，返回原函数）。

    参数即登记字段：worker = worker/入口名；reason = 豁免三要件如何满足；
    cadence = 批间节流节奏（配置项与默认值）。scripts/check_space_filter.py
    只认本装饰器与其内置豁免表，注释不构成豁免。
    """

    def decorator(func: F) -> F:
        return func

    return decorator
