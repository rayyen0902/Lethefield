"""注销流水线（M9/M10 统一五步 + 广播，严格按序）。

硬性顺序（红线 5 操作化，设计文档 §17.2/M10）：
1. 标记 destroying；
2. **驱逐计算实例**（ConfiguredGraphFactory.close + removeConfiguration——必须先于
   任何 DROP KEYSPACE，规避在线 DROP 导致的 Unknown CF 污染）；
3. 先派生物后本体：rms_vectors 文档 → DROP 图 keyspace → 删 Pulsar namespace →
   最后 DROP EX keyspace（任何中途失败都不至于"图没了、源也没了"）；
4. 训练管线销毁广播（M10 真实链路：契约 5 指令 → 训练 tenant 持久化控制 topic，
   生产者等 broker ack；**最终失败 → 告警 + 留痕并中止，禁止静默进入第 5 步**——
   映射保留 destroying，步骤 1–3 幂等，修复后重跑本流程即可续做）；
5. 清映射 + 全链路校验无残留。
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from cassandra.cluster import Session
from elasticsearch import Elasticsearch
from gremlin_python.driver.client import Client
from lethefield_clients import (
    MappingCache,
    MappingTableControlPlaneStore,
    SpaceStatus,
    keyspace_name,
    validate_space_id,
)
from lethefield_logschema import LogEvent
from lethefield_rms.vectors import VECTORS_INDEX
from pulsar import Client as PulsarClient

from lethefield_scheduler import pulsar_admin
from lethefield_scheduler.config import DDL_TIMEOUT_SECONDS as _DDL_TIMEOUT_SECONDS
from lethefield_scheduler.config import DEFAULT_CONFIG, SchedulerConfig
from lethefield_scheduler.destroy_broadcast import BroadcastError, make_broadcast

logger = logging.getLogger(__name__)


class DestroyError(RuntimeError):
    """注销后全链路校验发现残留。"""


@dataclass
class DestroyDeps:
    """注销流程依赖容器（构造时注入，测试逐项替换为 fake）。"""

    store: MappingTableControlPlaneStore
    gremlin: Client
    cell_session: Session
    ex_session: Session
    es: Elasticsearch
    config: SchedulerConfig = DEFAULT_CONFIG
    # M10：契约 5 广播的 Pulsar 通道；不传 broadcast_destroy 时必须有，
    # 否则第 4 步抛 BroadcastError 中止（禁止留空占位静默放行）
    pulsar: PulsarClient | None = None


def _resolve_broadcast(deps: DestroyDeps) -> Callable[[str], None]:
    """默认广播 = 契约 5 真实链路（等 broker ack）；无 Pulsar 通道直接失败。"""
    if deps.pulsar is None:
        raise BroadcastError("销毁广播无 Pulsar 通道（契约 5：禁止留空占位静默放行）")
    return make_broadcast(deps.pulsar, max_retries=deps.config.broadcast_max_retries)


def destroy_space(
    deps: DestroyDeps,
    space_id: str,
    *,
    broadcast_destroy: Callable[[str], None] | None = None,
    cache: MappingCache | None = None,
) -> None:
    """注销一个 space（五步 + 广播，严格按序）；结束态：存储/映射/缓存全无残留。

    残留校验失败抛 DestroyError（映射已清、存储有残留——运维按异常信息处理，
    不自动重试销毁，防半毁状态被静默）。
    """
    validate_space_id(space_id)
    mapping = deps.store.get_space_mapping(space_id)  # 未注册 fail-closed
    gname = mapping.space_id  # 图名 = space_id（M5 冻结契约）

    # 1. 标记 destroying（sweep 立即停扫：list_spaces 只给 active）
    deps.store.update_space_status(space_id, SpaceStatus.DESTROYING)
    if cache is not None:
        cache.invalidate(space_id)

    # 2. 驱逐计算实例（红线 5：必须先于任何 DROP KEYSPACE）。
    # close 对未 open 的图容错（幂等），removeConfiguration 严格——驱逐不完整
    # 直接中止，绝不带开着的图实例进 DROP。
    deps.gremlin.submit(
        "if (ConfiguredGraphFactory.getGraphNames().contains(gname)) { "
        "try { ConfiguredGraphFactory.close(gname) } catch (Exception ignored) {}; "
        "ConfiguredGraphFactory.removeConfiguration(gname) "
        "}; 'evicted'",
        {"gname": gname},
    ).all().result()

    # 3. 先派生物后本体：ES 向量文档 → RMS 图 keyspace → Pulsar namespace → EX keyspace
    deps.es.options(ignore_status=(404,)).delete_by_query(
        index=VECTORS_INDEX,
        query={"term": {"space_id": space_id}},
        routing=space_id,
        conflicts="proceed",
        refresh=True,
    )
    deps.cell_session.execute(f"DROP KEYSPACE IF EXISTS {gname}", timeout=_DDL_TIMEOUT_SECONDS)
    pulsar_admin.delete_namespace(deps.config.pulsar_admin_url, deps.config.pulsar_tenant, space_id)
    deps.ex_session.execute(
        f"DROP KEYSPACE IF EXISTS {keyspace_name(space_id)}", timeout=_DDL_TIMEOUT_SECONDS
    )

    # 4. 训练管线销毁广播（契约 5：真实链路、等 broker ack；最终失败 →
    #    告警 + 留痕并中止——禁止静默进入第 5 步，映射保留 destroying 可重试）
    broadcast = broadcast_destroy or _resolve_broadcast(deps)
    try:
        broadcast(space_id)
    except Exception:
        logger.error(
            LogEvent(
                service="lethefield-scheduler",
                event_type="destroy_broadcast_failed",
                space_id=space_id,
                payload={"consequence": "注销中止于第 4 步，映射保留 destroying，可重试"},
            ).to_jsonl()
        )
        raise

    # 5. 清映射 + 全链路校验无残留
    deps.store.unregister_space(space_id)
    residues = _find_residues(deps, space_id, gname)
    if residues:
        raise DestroyError(f"space {space_id} 注销后残留：{', '.join(residues)}")


def _find_residues(deps: DestroyDeps, space_id: str, gname: str) -> list[str]:
    residues: list[str] = []
    graph_names = deps.gremlin.submit("ConfiguredGraphFactory.getGraphNames()").all().result()
    if gname in graph_names:
        residues.append(f"图配置 {gname} 仍在 ConfiguredGraphFactory")
    row = deps.cell_session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
        (gname,),
    ).one()
    if row is not None:
        residues.append(f"图 keyspace {gname} 仍存在")
    row = deps.ex_session.execute(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
        (keyspace_name(space_id),),
    ).one()
    if row is not None:
        residues.append(f"EX keyspace {keyspace_name(space_id)} 仍存在")
    count = deps.es.options(ignore_status=(404,)).count(
        index=VECTORS_INDEX, query={"term": {"space_id": space_id}}, routing=space_id
    )
    if getattr(count, "status_code", 200) != 404 and count.body.get("count", 0) > 0:
        residues.append(f"rms_vectors 仍有 {count.body['count']} 条 space 文档")
    return residues
