"""M3 FF 计算引擎验收的集成测试（开发文档 §4 验收标准 2/3/4）。

- 端到端：s=0.9 但 Δn=20 的节点现算 s_effective≈0.10 被 θ 过滤（spike 已验证场景）
- 存储层巡检：s 只在 δ 触发时刻变化，两次 δ 之间任意读取不变（衰减未写回）
- s 截断时 ff_s_clamp_total{bound} 指标正确计数

另覆盖三条 δ 触发路径的图侧语义：
- reinforce +0.2：更新 n_last_touched、reinforce_count+1、立即重算 n_star_cached
- conflict −0.5：更新 n_last_touched、conflict_count+1
- neglect −0.1：不更新 n_last_touched（否则惩罚自我抵消）、neglect_count+1
"""

import uuid

import pytest
from conftest import GREMLIN_ALIAS, GREMLIN_URL
from lethefield_clients import gremlin_client
from lethefield_rms.ff import (
    apply_conflict,
    apply_neglect,
    apply_reinforce,
    n_star_horizon,
    read_phi,
    s_effective,
    theta_effective,
)
from lethefield_rms.schema import ensure_graph_schema
from lethefield_rms.writer import create_event_node
from prometheus_client import REGISTRY

SPACE = f"m3s-{uuid.uuid4().hex[:8]}"
TAU = 1_720_000_000_000

# 占位参数（与 ff_utils / FFConfig 默认同源）：θ_base=0.3、ρ=1
THETA_BASE = 0.3
RHO = 1.0


@pytest.fixture(scope="module")
def rms_graph():
    """唯一图名的全量 schema 图；清理只 close 图实例，不 DROP keyspace（红线 5）。"""
    gname = f"m3_{uuid.uuid4().hex[:8]}"
    client = gremlin_client(GREMLIN_URL, GREMLIN_ALIAS)
    ensure_graph_schema(client, gname)
    yield client, gname
    client.submit("ConfiguredGraphFactory.close(gname); 'closed'", {"gname": gname}).all().result()
    client.close()


def _create(client, gname, node_key, *, s, n_created):
    create_event_node(
        client,
        gname,
        node_key=node_key,
        space_id=SPACE,
        content=f"content of {node_key}",
        tau_ms=TAU,
        ref_ex=f"ex-{node_key}",
        s=s,
        n_created=n_created,
    )


def _clamp_metric(bound: str) -> float:
    return REGISTRY.get_sample_value("lethefield_ff_s_clamp_total", {"bound": bound}) or 0.0


def test_reinforce_path(rms_graph):
    client, gname = rms_graph
    _create(client, gname, "rf-1", s=0.5, n_created=100)

    updated = apply_reinforce(client, gname, space_id=SPACE, node_key="rf-1", n_now=120)

    assert updated.s == pytest.approx(0.7)  # 0.5 + 0.2
    assert updated.n_last_touched == 120  # reinforce 更新 n_last_touched
    assert updated.reinforce_count == 1
    # δ 调整立即重算 n_star_cached（绝对视界 = n_last_touched + ceil(n*)）
    assert updated.n_star_cached == n_star_horizon(0.7, 120, THETA_BASE)

    phi = read_phi(client, gname, space_id=SPACE, node_key="rf-1")
    assert phi == updated  # 落库值与引擎返回值一致


def test_conflict_path(rms_graph):
    client, gname = rms_graph
    _create(client, gname, "cf-1", s=0.8, n_created=100)

    updated = apply_conflict(client, gname, space_id=SPACE, node_key="cf-1", n_now=150)

    assert updated.s == pytest.approx(0.3)  # 0.8 − 0.5
    assert updated.n_last_touched == 150  # 冲突失效更新 n_last_touched
    assert updated.conflict_count == 1


def test_neglect_path_does_not_touch(rms_graph):
    client, gname = rms_graph
    _create(client, gname, "ng-1", s=0.6, n_created=100)

    updated = apply_neglect(client, gname, space_id=SPACE, node_key="ng-1", n_now=180)

    assert updated.s == pytest.approx(0.5)  # 0.6 − 0.1
    # 忽视惩罚不更新 n_last_touched——否则惩罚自我抵消（开发文档 §4 δ 表）
    assert updated.n_last_touched == 100
    assert updated.neglect_count == 1
    # n_star_cached 仍立即重算，基准是未被触碰的 n_last_touched
    assert updated.n_star_cached == n_star_horizon(0.5, 100, THETA_BASE)


def test_s_only_changes_at_delta_trigger(rms_graph):
    """存储层巡检：两次 δ 触发之间任意时间点读 s 不变（衰减永不写回存储）。"""
    client, gname = rms_graph
    _create(client, gname, "st-1", s=0.9, n_created=100)

    apply_reinforce(client, gname, space_id=SPACE, node_key="st-1", n_now=110)
    s_after_delta = read_phi(client, gname, space_id=SPACE, node_key="st-1").s
    assert s_after_delta == pytest.approx(1.0)  # 0.9 + 0.2 触顶截断

    # 期间发生"读取时现算"——现算本身不得有任何写回副作用
    phi = read_phi(client, gname, space_id=SPACE, node_key="st-1")
    _ = s_effective(phi.s, phi.n_last_touched, n_now=500)
    for _ in range(3):
        assert read_phi(client, gname, space_id=SPACE, node_key="st-1").s == s_after_delta


def test_decay_filter_scenario(rms_graph):
    """端到端：s=0.9 但 Δn=20 的节点现算 s_effective≈0.10，被 θ_effective 过滤。"""
    client, gname = rms_graph
    _create(client, gname, "df-hot", s=0.9, n_created=100)
    _create(client, gname, "df-decayed", s=0.9, n_created=80)
    n_now = 100

    theta = theta_effective(THETA_BASE, RHO)
    kept = set()
    for key in ("df-hot", "df-decayed"):
        phi = read_phi(client, gname, space_id=SPACE, node_key=key)
        if s_effective(phi.s, phi.n_last_touched, n_now) >= theta:
            kept.add(key)

    assert kept == {"df-hot"}  # Δn=0 高分召回；Δn=20 现算≈0.098 被过滤
    decayed_eff = s_effective(0.9, 80, n_now)
    assert 0.05 < decayed_eff < 0.15, f"spike 参考值 s_eff≈0.10，实测 {decayed_eff:.3f}"


def test_clamp_metric_upper(rms_graph):
    client, gname = rms_graph
    _create(client, gname, "cl-up", s=0.95, n_created=100)
    before = _clamp_metric("upper")

    updated = apply_reinforce(client, gname, space_id=SPACE, node_key="cl-up", n_now=101)

    assert updated.s == 1.0  # 0.95 + 0.2 触顶截断到 s_max
    assert _clamp_metric("upper") == before + 1


def test_clamp_metric_lower(rms_graph):
    client, gname = rms_graph
    _create(client, gname, "cl-lo", s=0.05, n_created=100)
    before = _clamp_metric("lower")

    updated = apply_conflict(client, gname, space_id=SPACE, node_key="cl-lo", n_now=101)

    assert updated.s == 0.0  # 0.05 − 0.5 触底截断到 s_min
    assert _clamp_metric("lower") == before + 1
