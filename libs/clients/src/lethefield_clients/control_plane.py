"""ControlPlaneStore 抽象接口（M0 冻结）+ M9 映射表正式实现。

设计依据：开发文档 §0.1 Cell 落地时机 + M9——
所有存储访问必须经过 ControlPlaneStore 抽象与 space→Cell 映射，
禁止"绕过映射直连默认集群"的快路径。接口（六个抽象方法）在本模块冻结。

元数据模型（M9 落地，设计文档 §17.2）：
    space_id → {cell_id, ex_cluster_id, pulsar_cluster_id, status, tier}
    cell_id  → {endpoints, capacity, watermark_state}
    存储于 `lethefield_control` 专用 keyspace（"Cell-1 专用 keyspace"的最小部署形态），
    直写 CQL（同 archive.py 规约），不经 JanusGraph。
    **映射表丢失 = 全部 space 失联**——备份/导出见 control_backup.py（1.0 验收硬指标）。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum

from cassandra.cluster import Session

from lethefield_clients.spaces import SpaceType, validate_space_id


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
    # M8：仅产品/运营维度标注（设计文档 §8），核心服务禁止按 space_type 分支
    space_type: SpaceType | None = None


@dataclass(frozen=True)
class CellInfo:
    cell_id: str
    # endpoints 以 JanusGraph 容器视角给出（建图配置直接使用）：
    # 如 {"cassandra": "cassandra-cell", "es": "es-graph"}
    endpoints: dict[str, str] = field(default_factory=dict)
    capacity: dict[str, float] = field(default_factory=dict)  # 各维度水位 0~1
    watermark_state: WatermarkState = WatermarkState.OPEN


class SpaceNotFoundError(KeyError):
    """映射表中不存在该 space。"""


LOCAL_CELL_ID = "cell-local"


def local_cell() -> CellInfo:
    """单节点起步部署的唯一 Cell：端点为 JanusGraph 容器视角的 compose 服务名
    （建图配置直接使用；host 侧访问仍走 factories 的 env 连接）。"""
    return CellInfo(
        cell_id=LOCAL_CELL_ID,
        endpoints={"cassandra": "cassandra-cell", "es": "es-graph"},
    )


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

    @abstractmethod
    def list_spaces(self) -> list[str]:
        """列出当前需要周期维护（sweep）的 space 集合（M6 定案新增）。

        M9 起正式实现按映射表 status=active 过滤；过渡期 EX keyspace 推导实现
        已随 M9 整体移除。
        """


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
        """本地开发默认：单 Cell 'cell-local'，端点为 JanusGraph 容器视角的 compose 服务名。"""
        return cls(local_cell())

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
            space_type=mapping.space_type,
        )

    def get_cell(self, cell_id: str) -> CellInfo:
        if cell_id != self._cell.cell_id:
            raise KeyError(cell_id)
        return self._cell

    def list_cells(self, watermark_state: WatermarkState | None = None) -> list[CellInfo]:
        if watermark_state is None or self._cell.watermark_state == watermark_state:
            return [self._cell]
        return []

    def list_spaces(self) -> list[str]:
        return sorted(self._spaces)


# ---------------------------------------------------------------- M9 映射表正式实现

# 控制面元数据 keyspace（"Cell-1 专用 keyspace"最小部署形态，设计文档 §17.2）；
# 与图 keyspace 物理分离——映射表丢失 = 全部 space 失联，备份/导出见 control_backup。
CONTROL_KEYSPACE = "lethefield_control"
SPACES_TABLE = "spaces"
CELLS_TABLE = "cells"


class MappingTableControlPlaneStore(ControlPlaneStore):
    """M9 正式实现：space→Cell 映射持久化于 `lethefield_control` keyspace（直写 CQL）。

    - `list_spaces()` 按 status=active 过滤（替换过渡期 EX keyspace 推导语义；
      destroying 中的 space 不再被 sweep 扫入，M6 已知过渡偏差收敛）。
    - 映射行按主键覆盖写（register 幂等）；space_id 经 spaces.validate_space_id 校验。
    - Cell 管理（register_cell/update_cell_watermark）与 unregister_space/list_space_mappings
      是本具体类的扩展方法，不进 M0 冻结的 ABC——调度器依赖具体类。
    """

    def __init__(self, session: Session) -> None:
        self._session = session

    # ---------------------------------------------------------- 建表 / 引导

    def ensure_tables(self) -> None:
        """幂等建控制面 keyspace + 两表（bootstrap 与调用方启动时调用）。"""
        self._session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {CONTROL_KEYSPACE} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        self._session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_KEYSPACE}.{SPACES_TABLE} (
                space_id text PRIMARY KEY,
                cell_id text,
                ex_cluster_id text,
                pulsar_cluster_id text,
                status text,
                tier text,
                space_type text
            )
            """
        )
        self._session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CONTROL_KEYSPACE}.{CELLS_TABLE} (
                cell_id text PRIMARY KEY,
                endpoints map<text, text>,
                capacity map<text, double>,
                watermark_state text
            )
            """
        )

    # ---------------------------------------------------------- ABC 方法

    def get_space_mapping(self, space_id: str) -> SpaceMapping:
        row = self._session.execute(
            f"SELECT space_id, cell_id, ex_cluster_id, pulsar_cluster_id, status, tier, "
            f"space_type FROM {CONTROL_KEYSPACE}.{SPACES_TABLE} WHERE space_id = %s",
            (space_id,),
        ).one()
        if row is None:
            raise SpaceNotFoundError(space_id)
        return _row_to_mapping(row)

    def register_space(self, mapping: SpaceMapping) -> None:
        validate_space_id(mapping.space_id)
        self._session.execute(
            f"INSERT INTO {CONTROL_KEYSPACE}.{SPACES_TABLE} "
            "(space_id, cell_id, ex_cluster_id, pulsar_cluster_id, status, tier, space_type) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                mapping.space_id,
                mapping.cell_id,
                mapping.ex_cluster_id,
                mapping.pulsar_cluster_id,
                mapping.status.value,
                mapping.tier.value,
                mapping.space_type.value if mapping.space_type is not None else None,
            ),
        )

    def update_space_status(self, space_id: str, status: SpaceStatus) -> None:
        self.get_space_mapping(space_id)  # 不存在则 fail-closed
        self._session.execute(
            f"UPDATE {CONTROL_KEYSPACE}.{SPACES_TABLE} SET status = %s WHERE space_id = %s",
            (status.value, space_id),
        )

    def get_cell(self, cell_id: str) -> CellInfo:
        row = self._session.execute(
            f"SELECT cell_id, endpoints, capacity, watermark_state "
            f"FROM {CONTROL_KEYSPACE}.{CELLS_TABLE} WHERE cell_id = %s",
            (cell_id,),
        ).one()
        if row is None:
            raise KeyError(cell_id)
        return _row_to_cell(row)

    def list_cells(self, watermark_state: WatermarkState | None = None) -> list[CellInfo]:
        rows = self._session.execute(
            f"SELECT cell_id, endpoints, capacity, watermark_state "
            f"FROM {CONTROL_KEYSPACE}.{CELLS_TABLE}"
        ).all()
        cells = [_row_to_cell(row) for row in rows]
        if watermark_state is not None:
            cells = [c for c in cells if c.watermark_state == watermark_state]
        return sorted(cells, key=lambda c: c.cell_id)

    def list_spaces(self) -> list[str]:
        """status=active 的 space 集合（status 无索引，控制面规模 client 侧过滤）。"""
        rows = self._session.execute(
            f"SELECT space_id, status FROM {CONTROL_KEYSPACE}.{SPACES_TABLE}"
        ).all()
        return sorted(row.space_id for row in rows if row.status == SpaceStatus.ACTIVE.value)

    # ---------------------------------------------------------- 具体类扩展（调度器用）

    def register_cell(self, cell: CellInfo) -> None:
        self._session.execute(
            f"INSERT INTO {CONTROL_KEYSPACE}.{CELLS_TABLE} "
            "(cell_id, endpoints, capacity, watermark_state) VALUES (%s, %s, %s, %s)",
            (
                cell.cell_id,
                dict(cell.endpoints),
                dict(cell.capacity),
                cell.watermark_state.value,
            ),
        )

    def update_cell_watermark(
        self, cell_id: str, capacity: dict[str, float], state: WatermarkState
    ) -> None:
        self.get_cell(cell_id)  # 不存在则 fail-closed
        self._session.execute(
            f"UPDATE {CONTROL_KEYSPACE}.{CELLS_TABLE} "
            "SET capacity = %s, watermark_state = %s WHERE cell_id = %s",
            (dict(capacity), state.value, cell_id),
        )

    def unregister_space(self, space_id: str) -> None:
        """注销流程末步清除映射（先删存储后清映射，顺序由调度器保证）。"""
        self.get_space_mapping(space_id)  # 不存在则 fail-closed
        self._session.execute(
            f"DELETE FROM {CONTROL_KEYSPACE}.{SPACES_TABLE} WHERE space_id = %s",
            (space_id,),
        )

    def list_space_mappings(self) -> list[SpaceMapping]:
        """全量映射（备份/导出走此；控制面规模，不涉及业务数据扫描）。"""
        rows = self._session.execute(
            f"SELECT space_id, cell_id, ex_cluster_id, pulsar_cluster_id, status, tier, "
            f"space_type FROM {CONTROL_KEYSPACE}.{SPACES_TABLE}"
        ).all()
        return sorted((_row_to_mapping(row) for row in rows), key=lambda m: m.space_id)


def _row_to_mapping(row) -> SpaceMapping:
    return SpaceMapping(
        space_id=row.space_id,
        cell_id=row.cell_id,
        ex_cluster_id=row.ex_cluster_id,
        pulsar_cluster_id=row.pulsar_cluster_id,
        status=SpaceStatus(row.status),
        tier=Tier(row.tier),
        space_type=SpaceType(row.space_type) if row.space_type is not None else None,
    )


def _row_to_cell(row) -> CellInfo:
    return CellInfo(
        cell_id=row.cell_id,
        endpoints=dict(row.endpoints or {}),
        capacity=dict(row.capacity or {}),
        watermark_state=WatermarkState(row.watermark_state),
    )
