"""M17 命令业务逻辑单测（fake deps，不起栈）。"""

from dataclasses import replace

import pytest
from lethefield_clients import (
    CellInfo,
    SpaceMapping,
    SpaceNotFoundError,
    StaticControlPlaneStore,
    Tier,
    local_cell,
)
from lethefield_ops_cli import commands
from lethefield_scheduler.migrate import MigrationReport


class FakeStore(StaticControlPlaneStore):
    """StaticControlPlaneStore + MappingTable 扩展方法（update_space_tier/register_cell）。"""

    def __init__(self) -> None:
        super().__init__(local_cell())
        self.tier_updates: list[tuple[str, Tier]] = []
        self.registered_cells: list[CellInfo] = []

    def register_space(self, mapping: SpaceMapping) -> None:
        # 绕过 Static 的单 Cell 限制：测试需要登记归属其他 Cell 的 space
        self._spaces[mapping.space_id] = mapping

    def update_space_tier(self, space_id: str, tier: Tier) -> None:
        mapping = self.get_space_mapping(space_id)  # fail-closed
        self._spaces[space_id] = replace(mapping, tier=tier)
        self.tier_updates.append((space_id, tier))

    def register_cell(self, cell: CellInfo) -> None:
        self.registered_cells.append(cell)


def _mapping(space_id: str, cell_id: str = "cell-local") -> SpaceMapping:
    return SpaceMapping(
        space_id=space_id,
        cell_id=cell_id,
        ex_cluster_id="ex-local",
        pulsar_cluster_id="pulsar-local",
    )


@pytest.fixture
def store():
    return FakeStore()


# ------------------------------------------------------------------ space status / set-tier


class FakeCounters:
    def vertex_count(self, gname: str) -> int:
        return 3

    def edge_count(self, gname: str) -> int:
        return 2

    def vector_count(self, space_id: str) -> int:
        return 1


def test_space_status(store):
    store.register_space(_mapping("s1"))
    result = commands.cmd_space_status(store, FakeCounters(), ["s1"])
    assert result.exit_code == 0
    text = "\n".join(result.lines)
    assert "cell=cell-local" in text and "tier=" in text
    assert "vertices=3 edges=2 vectors=1" in text
    assert "近似" in text  # 配额用量近似语义必须注明


def test_space_status_unknown_space_fails(store):
    with pytest.raises(SpaceNotFoundError):
        commands.cmd_space_status(store, FakeCounters(), ["nope"])


def test_set_tier(store):
    store.register_space(_mapping("s1"))
    result = commands.cmd_set_tier(store, "s1", Tier.PREMIUM)
    assert result.exit_code == 0
    assert store.tier_updates == [("s1", Tier.PREMIUM)]
    assert store.get_space_mapping("s1").tier == Tier.PREMIUM


def test_set_tier_unknown_space_fail_closed(store):
    with pytest.raises(SpaceNotFoundError):
        commands.cmd_set_tier(store, "nope", Tier.HOT)
    assert store.tier_updates == []


# ------------------------------------------------------------------ destroy


class FakeGremlin:
    def __init__(self) -> None:
        self.closed = False

    def close(self):
        self.closed = True


class FakeCluster:
    def __init__(self) -> None:
        self.shutdown_called = False

    def connect(self):
        return object()

    def shutdown(self):
        self.shutdown_called = True


class FakePulsar:
    def __init__(self) -> None:
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def fake_factories(monkeypatch):
    monkeypatch.setattr(commands, "gremlin_client", lambda *a, **k: FakeGremlin())
    monkeypatch.setattr(commands, "ex_cassandra_cluster", lambda *a, **k: FakeCluster())
    monkeypatch.setattr(commands, "cassandra_cluster", lambda *a, **k: FakeCluster())
    monkeypatch.setattr(commands, "es_client", lambda *a, **k: object())
    monkeypatch.setattr(commands, "pulsar_client", lambda *a, **k: FakePulsar())


def test_destroy_triggers_pipeline(store, fake_factories, monkeypatch):
    calls = []
    monkeypatch.setattr(
        commands, "destroy_space", lambda deps, space_id: calls.append((deps, space_id))
    )
    result = commands.cmd_destroy(store, FakeCluster(), "s1")
    assert result.exit_code == 0
    assert "已注销" in result.detail
    assert len(calls) == 1
    deps, space_id = calls[0]
    assert space_id == "s1"
    assert deps.pulsar is not None  # 契约 5 广播通道必须有（M10：缺失则第 4 步中止）


# ------------------------------------------------------------------ migrate


def _report(space_id: str, source: str, target: str) -> MigrationReport:
    return MigrationReport(
        space_id=space_id,
        source_cell_id=source,
        target_cell_id=target,
        read_only_window_seconds=1.5,
        ex_experience_rows=0,
        ex_meta_rows=0,
        rms_vertices=0,
        rms_edges=0,
        vector_docs=0,
        step_seconds={"total": 2.0},
    )


@pytest.fixture
def fake_migrate(store, fake_factories, monkeypatch):
    calls = []

    def fake_migrate_space(deps, space_id, *, to_cell_id=None, cache=None, grace_seconds=0.0):
        calls.append({"space_id": space_id, "to_cell_id": to_cell_id})
        source = store.get_space_mapping(space_id).cell_id
        return _report(space_id, source, to_cell_id or "cell-auto")

    monkeypatch.setattr(commands, "migrate_space", fake_migrate_space)
    selected = []

    def fake_select_cell(store_, *, exclude=frozenset()):
        selected.append(exclude)
        return CellInfo(cell_id="cell-auto")

    monkeypatch.setattr(commands, "select_cell", fake_select_cell)
    return calls, selected


def test_migrate_rebalance_excludes_current_cell(store, fake_migrate):
    store.register_space(_mapping("s1"))
    calls, selected = fake_migrate
    result = commands.cmd_migrate_rebalance(store, "s1")
    assert result.exit_code == 0
    assert "cell-auto" in result.detail
    assert calls == [{"space_id": "s1", "to_cell_id": None}]
    assert selected == [frozenset({"cell-local"})]  # 再平衡排除当前 Cell


def test_migrate_to_cell_explicit_target(store, fake_migrate):
    store.register_space(_mapping("s1"))
    calls, selected = fake_migrate
    result = commands.cmd_migrate_to_cell(store, "s1", "cell-2")
    assert result.exit_code == 0
    assert calls == [{"space_id": "s1", "to_cell_id": "cell-2"}]
    assert selected == []  # 显式目标不走自动选 Cell


def test_migrate_evacuate_migrates_each_space(store, fake_migrate):
    store.register_space(_mapping("s1"))
    store.register_space(_mapping("s2"))
    calls, _ = fake_migrate
    result = commands.cmd_migrate_evacuate(store, "cell-local", ["s1", "s2"])
    assert result.exit_code == 0
    assert [c["space_id"] for c in calls] == ["s1", "s2"]


def test_migrate_evacuate_rejects_foreign_space(store, fake_migrate):
    store.register_space(_mapping("s1", cell_id="cell-elsewhere"))
    calls, _ = fake_migrate
    with pytest.raises(ValueError, match="不在待退役 Cell"):
        commands.cmd_migrate_evacuate(store, "cell-local", ["s1"])
    assert calls == []  # 校验失败零迁移


# ------------------------------------------------------------------ auth revoke


class FakeRegistry:
    def __init__(self, existed: bool) -> None:
        self.existed = existed
        self.revoked: list[str] = []

    def revoke(self, space_ref: str) -> bool:
        self.revoked.append(space_ref)
        return self.existed


class FakeHotStore:
    def __init__(self, count: int = 5) -> None:
        self.count = count
        self.scrubbed: list[str] = []

    def scrub(self, space_ref: str) -> int:
        self.scrubbed.append(space_ref)
        return self.count


def test_auth_revoke_full_flow():
    registry, hot = FakeRegistry(existed=True), FakeHotStore(count=5)
    result = commands.cmd_auth_revoke(registry, hot, "s1")
    assert result.exit_code == 0
    assert registry.revoked == hot.scrubbed  # 同一 space_ref（space_ref_of 单点转换）
    assert "存量处置 5 条" in result.detail


def test_auth_revoke_unknown_entry_reports_not_found():
    registry, hot = FakeRegistry(existed=False), FakeHotStore(count=0)
    result = commands.cmd_auth_revoke(registry, hot, "ghost")
    assert result.exit_code == 1  # IS CLI 同口径 not found
    assert "无条目" in result.detail
    assert hot.scrubbed  # scrub 幂等照跑（存量处置不因无条目跳过）


# ------------------------------------------------------------------ cell watermark / register


def test_cell_watermark_view(store):
    result = commands.cmd_cell_watermark(store, "cell-local")
    assert result.exit_code == 0
    assert "state=" in result.lines[0]


def test_cell_watermark_unknown_cell_fails(store):
    with pytest.raises(KeyError):
        commands.cmd_cell_watermark(store, "cell-nope")


def test_cell_watermark_refresh(store, fake_factories, monkeypatch):
    refreshed = []

    def fake_refresh(store_, cell_id, *, probe=None, cell_session=None, es=None, config=None):
        refreshed.append(cell_id)
        return CellInfo(cell_id=cell_id, capacity={"keyspaces": 0.1})

    monkeypatch.setattr(commands, "refresh_cell", fake_refresh)
    result = commands.cmd_cell_watermark(
        store, "cell-local", refresh=True, cell_cluster=FakeCluster()
    )
    assert result.exit_code == 0
    assert refreshed == ["cell-local"]


def test_cell_register_success(store):
    result = commands.cmd_cell_register(store, "cell-2", {"cassandra": "c2-cas", "es": "c2-es"})
    assert result.exit_code == 0
    assert store.registered_cells[0].cell_id == "cell-2"
    assert store.registered_cells[0].endpoints == {"cassandra": "c2-cas", "es": "c2-es"}


def test_cell_register_missing_required_keys(store):
    with pytest.raises(ValueError, match="缺少必需键"):
        commands.cmd_cell_register(store, "cell-2", {"cassandra": "c2-cas"})
    assert store.registered_cells == []
