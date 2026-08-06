"""space_id 字符集约束与 space_type 枚举（M8 正式定义，单点）。

space_id 是顶层分区键，同一约束同时约束四种存储命名：EX keyspace（`ex_{space_id}`）、
RMS 图名（= space_id）、ES routing、Pulsar namespace（M10）。因此单点放在 libs/clients
独立模块，不属于 ex_n 的 EX 序号语义。

fail-closed 语义（M5 起沿用，M8 转正为定案）：不满足约束直接拒绝，不静默改写——
静默改写会让两个不同 space_id 映射到同一存储名，造成跨 space 数据混流。

space_type（设计文档 §8）：仅产品/运营维度标注（companion=陪伴型 / project=项目型），
**不得影响 RMS/FS/SS 核心逻辑**——核心服务代码禁止出现按 space_type 分支的业务逻辑
（scripts/check_space_model.py 巡检强制）。
"""

from enum import StrEnum

SPACE_ID_MAX_LEN = 40


def validate_space_id(space_id: str) -> str:
    """校验 space_id 字符集约束 [a-z0-9_]、≤40 字符；不合法抛 ValueError，合法原样返回。"""
    if (
        not space_id
        or len(space_id) > SPACE_ID_MAX_LEN
        or not all(c.islower() or c.isdigit() or c == "_" for c in space_id)
    ):
        raise ValueError(
            f"space_id {space_id!r} 不满足命名约束：[a-z0-9_]、≤{SPACE_ID_MAX_LEN} 字符"
        )
    return space_id


class SpaceType(StrEnum):
    """空间语义标注（仅产品/运营维度，核心服务禁止按其分支）。"""

    COMPANION = "companion"
    PROJECT = "project"
