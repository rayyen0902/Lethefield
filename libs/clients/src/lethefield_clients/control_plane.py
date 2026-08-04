"""ControlPlaneStore 抽象接口（M0 冻结）。

设计依据：开发文档 §0.1 Cell 落地时机 + M9——
所有存储访问必须经过 ControlPlaneStore 抽象与 space→Cell 映射，
禁止"绕过映射直连默认集群"的快路径。接口在本模块冻结，
正式实现（调度器元数据存储）随 M9 落地。

元数据模型（M9）：
    space_id → {cell_id, ex_cluster_id, pulsar_cluster_id, status, tier}
    cell_id  → {endpoints, capacity, watermark_state}
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum


class SpaceStatus(StrEnum):
    ACTIVE = "active"
    MIGRATING = "migrating"
    DESTROYING = "destroying"


class Tier(StrEnum):
    COLD = "cold"
    HOT = "hot"
    PREMIUM = "premium"


class WatermarkState(StrEnum):
    OPEN = "open"
    FILLING = "filling"
    CLOSED = "closed"


@dataclass(frozen=True)
class SpaceMapping:
    space_id: str
    cell_id: str
    ex_cluster_id: str
    pulsar_cluster_id: str
    status: SpaceStatus = SpaceStatus.ACTIVE
    tier: Tier = Tier.COLD


@dataclass(frozen=True)
class CellInfo:
    cell_id: str
    endpoints: dict[str, str] = field(default_factory=dict)  # 如 {"cassandra": ..., "es": ...}
    capacity: dict[str, float] = field(default_factory=dict)  # 各维度水位 0~1
    watermark_state: WatermarkState = WatermarkState.OPEN


class SpaceNotFoundError(KeyError):
    """映射表中不存在该 space。"""


class ControlPlaneStore(ABC):
    """space→Cell 映射的唯一访问入口（M0 冻结的方法集）。"""

    @abstractmethod
    def get_space_mapping(self, space_id: str) -> SpaceMapping:
        """返回 space 的归属映射；不存在抛 SpaceNotFoundError。"""

    @abstractmethod
    def register_space(self, mapping: SpaceMapping) -> None:
        """注册 space→Cell 映射（开通流程最后一步，先存储后注册）。"""

    @abstractmethod
    def update_space_status(self, space_id: str, status: SpaceStatus) -> None:
        """更新 space 状态（active/migrating/destroying）。"""

    @abstractmethod
    def get_cell(self, cell_id: str) -> CellInfo:
        """返回 Cell 信息；不存在抛 KeyError。"""

    @abstractmethod
    def list_cells(self, watermark_state: WatermarkState | None = None) -> list[CellInfo]:
        """列出 Cell，可按水位状态过滤（调度选水位最低的 open Cell 用）。"""


class StaticControlPlaneStore(ControlPlaneStore):
    """单节点起步部署形态的开发用实现：单一本地 Cell，内存映射表。

    对应开发文档"代码按 Cell 最终形态实现，部署按最小规模起步"：
    代码路径全部走 ControlPlaneStore 抽象，本实现把所有 space 映射到
    唯一的本地 Cell。M9 落地正式实现（调度器 + 元数据存储）后替换。
    """

    def __init__(self, cell: CellInfo) -> None:
        self._cell = cell
        self._spaces: dict[str, SpaceMapping] = {}

    @classmethod
    def local(cls) -> "StaticControlPlaneStore":
        """本地开发默认：单 Cell 'cell-local'，端点指向 compose 服务。"""
        return cls(
            CellInfo(
                cell_id="cell-local",
                endpoints={
                    "cassandra": "localhost:9042",
                    "es": "http://localhost:9200",
                },
            )
        )

    def get_space_mapping(self, space_id: str) -> SpaceMapping:
        try:
            return self._spaces[space_id]
        except KeyError:
            raise SpaceNotFoundError(space_id) from None

    def register_space(self, mapping: SpaceMapping) -> None:
        if mapping.cell_id != self._cell.cell_id:
            raise ValueError(
                f"StaticControlPlaneStore 只服务单一 Cell {self._cell.cell_id!r}，"
                f"拒绝注册指向 {mapping.cell_id!r} 的映射"
            )
        self._spaces[mapping.space_id] = mapping

    def update_space_status(self, space_id: str, status: SpaceStatus) -> None:
        mapping = self.get_space_mapping(space_id)
        self._spaces[space_id] = SpaceMapping(
            space_id=mapping.space_id,
            cell_id=mapping.cell_id,
            ex_cluster_id=mapping.ex_cluster_id,
            pulsar_cluster_id=mapping.pulsar_cluster_id,
            status=status,
            tier=mapping.tier,
        )

    def get_cell(self, cell_id: str) -> CellInfo:
        if cell_id != self._cell.cell_id:
            raise KeyError(cell_id)
        return self._cell

    def list_cells(self, watermark_state: WatermarkState | None = None) -> list[CellInfo]:
        if watermark_state is None or self._cell.watermark_state == watermark_state:
            return [self._cell]
        return []
