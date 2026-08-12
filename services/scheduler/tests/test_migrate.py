"""跨 Cell 迁移流水线单测：步骤编排 / 失败回滚 / 清理残留（fake 依赖，不起栈）。

端到端真实组件演练在 tests/integration/test_m10_migration_drill.py（cell2 profile）。
"""

from types import SimpleNamespace

import pytest
from lethefield_clients import (
    CellInfo,
    SpaceMapping,
    SpaceNotFoundError,
    SpaceStatus,
)
from lethefield_scheduler import migrate as migrate_mod
from lethefield_scheduler.migrate import (
    MigrateDeps,
    MigrationCleanupError,
    MigrationError,
    migrate_space,
)


class _FakeStore:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self._spaces = {
            "sp1": SpaceMapping(
                space_id="sp1", cell_id="cell-1", ex_cluster_id="ex-local", pulsar_cluster_id="p"
            )
        }
        self._cells = {
            "cell-1": CellInfo(cell_id="cell-1", endpoints={"cassandra": "c1", "es": "e1"}),
            "cell-2": CellInfo(cell_id="cell-2", endpoints={"cassandra": "c2", "es": "e2"}),
        }

    def get_space_mapping(self, space_id):
        try:
            return self._spaces[space_id]
        except KeyError:
            raise SpaceNotFoundError(space_id) from None

    def get_cell(self, cell_id):
        return self._cells[cell_id]

    def list_cells(self, state=None):
        cells = list(self._cells.values())
        if state is not None:
            cells = [c for c in cells if c.watermark_state == state]
        return cells

    def update_space_status(self, space_id, status):
        self._events.append(f"status:{status}")
        m = self._spaces[space_id]
        self._spaces[space_id] = SpaceMapping(
            space_id=m.space_id,
            cell_id=m.cell_id,
            ex_cluster_id=m.ex_cluster_id,
            pulsar_cluster_id=m.pulsar_cluster_id,
            status=status,
            tier=m.tier,
        )

    def update_space_cell(self, space_id, cell_id, ex_cluster_id):
        self._events.append(f"cutover:{cell_id}")
        m = self._spaces[space_id]
        self._spaces[space_id] = SpaceMapping(
            space_id=m.space_id,
            cell_id=cell_id,
            ex_cluster_id=ex_cluster_id,
            pulsar_cluster_id=m.pulsar_cluster_id,
            status=m.status,
            tier=m.tier,
        )


class _FakeResult:
    def __init__(self, value) -> None:
        self._value = value

    def all(self):
        return self

    def result(self):
        return self._value


class _Rs:
    """cassandra ResultSet 形态：.all() → list、.one() → row|None。"""

    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self):
        return self._rows

    def one(self):
        return self._rows[0] if self._rows else None


class _FakeGremlin:
    """源/目标图实例：submit 按脚本内容分发为计数/驱逐事件。"""

    def __init__(self, events: list[str], tag: str, vertices: int = 5, edges: int = 4) -> None:
        self._events = events
        self._tag = tag
        self._counts = {"V": vertices, "E": edges}

    def submit(self, script, bindings=None):
        if "V().count()" in script:
            return _FakeResult([self._counts["V"]])
        if "E().count()" in script:
            return _FakeResult([self._counts["E"]])
        assert "removeConfiguration" in script  # 驱逐必须带 removeConfiguration
        self._events.append(f"evict:{self._tag}")
        return _FakeResult(["evicted"])


class _FakeSession:
    """编排测试用 session：记录 DDL 事件 + 维护 keyspace 存在性（回滚路径用）。"""

    def __init__(self, events: list[str], tag: str) -> None:
        self._events = events
        self._tag = tag
        self.keyspaces: set[str] = set()

    def execute(self, statement, parameters=None, **kwargs):
        s = " ".join(statement.split())
        if s.startswith("DROP KEYSPACE"):
            self._events.append(f"drop:{self._tag}")
            self.keyspaces.discard(s.split()[-1])
            return _Rs([])
        if s.startswith("CREATE KEYSPACE"):
            self.keyspaces.add(s.split("EXISTS")[1].split("WITH")[0].strip())
            return _Rs([])
        if s.startswith("CREATE TABLE"):
            return _Rs([])
        if "system_schema.keyspaces" in s:
            ks = parameters[0]
            return _Rs([SimpleNamespace(keyspace_name=ks)] if ks in self.keyspaces else [])
        if s.startswith(("SELECT", "INSERT")):
            return _Rs([])  # 编排测试不关心行内容
        raise AssertionError(f"未预期语句：{s}")


class _FakeEs:
    def __init__(self, events: list[str], tag: str) -> None:
        self._events = events
        self._tag = tag

    def options(self, ignore_status=()):
        return self

    def delete_by_query(self, **kwargs):
        self._events.append(f"es:delete:{self._tag}")


def _deps(events, *, target_vertices=5, target_edges=4) -> MigrateDeps:
    return MigrateDeps(
        store=_FakeStore(events),
        source_gremlin=_FakeGremlin(events, "source"),
        target_gremlin=_FakeGremlin(events, "target", vertices=target_vertices, edges=target_edges),
        source_cell_session=_FakeSession(events, "src_cell"),
        target_cell_session=_FakeSession(events, "dst_cell"),
        source_es=_FakeEs(events, "src_es"),
        target_es=_FakeEs(events, "dst_es"),
        ex_session=_FakeSession(events, "ex"),
    )


class _Events(list):
    """事件记录列表，可挂附加属性（pipeline fixture 的 rebuild_calls）。"""


@pytest.fixture
def pipeline(monkeypatch):
    """把 RMS 重建/向量复制/EX 迁移替换为事件记录器（编排测试；各自的真实实现另有覆盖）。

    rebuild 返回 5v/4e 的 fake 计划（与 _FakeGremlin 默认计数一致）——等价校验
    对齐"目标图 == EX 重放计划"语义（M10 定案：EX 是唯一 source of truth）。
    rebuild_calls 记录 rebuild_space 实参（M13：归档 v_i lookup 必须走源侧注入）。
    """
    events = _Events()
    rebuild_calls: list[tuple] = []
    events.rebuild_calls = rebuild_calls
    plan = SimpleNamespace(
        nodes=[SimpleNamespace(node_key=f"n{i}") for i in range(5)],
        temporal_edges=[(f"n{i}", f"n{i + 1}") for i in range(4)],
        supersedes_edges=[],
    )

    def fake_rebuild(*a, **kw):
        events.append("build:rebuild")
        rebuild_calls.append((a, kw))
        return plan

    monkeypatch.setattr(
        migrate_mod, "ensure_graph_schema", lambda *a, **kw: events.append("build:graph")
    )
    monkeypatch.setattr(migrate_mod.rebuild, "rebuild_space", fake_rebuild)
    monkeypatch.setattr(migrate_mod, "_copy_vectors", lambda *a: events.append("copy:vectors") or 7)
    monkeypatch.setattr(
        migrate_mod, "_migrate_ex_same_cluster", lambda *a: events.append("copy:ex") or (3, 2)
    )
    return events


def test_happy_path_order_and_report(pipeline):
    events = pipeline
    ticks = iter(range(0, 1000))  # 假时钟：每步 +1s
    report = migrate_space(_deps(events), "sp1", grace_seconds=0, clock=lambda: float(next(ticks)))
    assert events == [
        f"status:{SpaceStatus.MIGRATING}",  # 只读窗口起点
        "build:graph",
        "build:rebuild",
        "copy:vectors",
        "copy:ex",
        "cutover:cell-2",  # 切映射
        f"status:{SpaceStatus.ACTIVE}",  # 只读窗口终点
        "evict:source",  # 红线 5：驱逐先于 DROP
        "drop:src_cell",
        "es:delete:src_es",
    ]
    assert report.target_cell_id == "cell-2"
    assert report.read_only_window_seconds > 0
    assert (report.ex_experience_rows, report.ex_meta_rows) == (3, 2)
    assert report.vector_docs == 7
    assert (report.rms_vertices, report.rms_edges) == (5, 4)
    assert set(report.step_seconds) >= {"rms_rebuild", "vector_copy", "ex_copy", "cutover"}


def test_non_active_status_rejected():
    events: list[str] = []
    deps = _deps(events)
    deps.store.update_space_status("sp1", SpaceStatus.DESTROYING)
    events.clear()
    with pytest.raises(MigrationError, match="不可迁移"):
        migrate_space(deps, "sp1")
    assert events == []  # 零副作用


def test_same_cell_target_rejected():
    events: list[str] = []
    with pytest.raises(MigrationError, match="迁到自己不算迁移"):
        migrate_space(_deps(events), "sp1", to_cell_id="cell-1")
    assert events == []


def test_auto_select_excludes_source(pipeline):
    events = pipeline
    report = migrate_space(_deps(events), "sp1")
    assert report.target_cell_id == "cell-2"  # select_cell(exclude={cell-1})


def test_rebuild_gets_source_side_vector_lookup(pipeline):
    """M13 红线 3：迁移链 rebuild_space 的归档 v_i lookup 走源侧（源 Cell session + 源 ES）。

    目标 Cell 上既没有旧 archived_nodes 快照、rms_vectors 文档也还没复制过去——
    不注源侧句柄归档快照会全落 v=None。
    """
    events = pipeline
    deps = _deps(events)
    migrate_space(deps, "sp1", grace_seconds=0)
    assert len(events.rebuild_calls) == 1
    _, kw = events.rebuild_calls[0]
    assert kw["es"] is deps.source_es
    assert kw["source_cell_session"] is deps.source_cell_session


def test_verify_mismatch_rolls_back(pipeline, monkeypatch):
    events = pipeline
    deps = _deps(events, target_vertices=4)  # 目标图顶点数与源不符
    with pytest.raises(MigrationError, match="等价校验失败"):
        migrate_space(deps, "sp1")
    assert "cutover:cell-2" not in events  # 未切映射
    # 回滚：目标侧半成品清理 + status 回 active（源仍服务）
    assert "evict:target" in events
    assert "drop:dst_cell" in events
    assert "es:delete:dst_es" in events
    assert events[-1] == f"status:{SpaceStatus.ACTIVE}"
    assert deps.store.get_space_mapping("sp1").cell_id == "cell-1"  # 映射未动


def test_cleanup_residue_raises_after_cutover(pipeline, monkeypatch):
    events = pipeline
    deps = _deps(events)

    def failing_evict(gremlin, gname):
        raise RuntimeError("gremlin 连接断")

    monkeypatch.setattr(migrate_mod, "_evict_graph", failing_evict)
    with pytest.raises(MigrationCleanupError, match="清理残留"):
        migrate_space(deps, "sp1")
    assert "cutover:cell-2" in events  # 切映射已完成、目标在服务
    assert deps.store.get_space_mapping("sp1").cell_id == "cell-2"
    assert deps.store.get_space_mapping("sp1").status == SpaceStatus.ACTIVE


# ------------------------------------------------- EX 复制原语（fake session 真实走逻辑）


class _ExFakeSession:
    """内存 keyspace/table 模拟：支撑 _copy_ex_keyspace 的 DDL + 逐行拷贝 + 计数。"""

    def __init__(self) -> None:
        self.tables: dict[str, list[tuple]] = {}

    def execute(self, statement, parameters=None, **kwargs):
        s = " ".join(statement.split())
        if s.startswith("CREATE KEYSPACE"):
            ks = s.split("EXISTS")[1].split("WITH")[0].strip()
            self.tables.setdefault(f"{ks}.experience_events", [])
            self.tables.setdefault(f"{ks}.meta_events", [])
            return _Rs([])
        if s.startswith("CREATE TABLE"):
            return _Rs([])
        if s.startswith("DROP KEYSPACE"):
            ks = s.split()[-1]
            self.tables = {k: v for k, v in self.tables.items() if not k.startswith(f"{ks}.")}
            return _Rs([])
        if s.startswith("SELECT"):
            cols_part = s[len("SELECT ") : s.index(" FROM ")].strip()
            table = s.split(" FROM ")[1].strip()
            if "COUNT(*)" in cols_part:
                return _Rs([SimpleNamespace(c=len(self.tables.get(table, [])))])
            cols = [c.strip() for c in cols_part.split(",")]
            return _Rs(
                [
                    SimpleNamespace(**dict(zip(cols, row, strict=False)))
                    for row in self.tables.get(table, [])
                ]
            )
        if s.startswith("INSERT"):
            table = s.split("INTO")[1].split("(")[0].strip()
            self.tables.setdefault(table, []).append(parameters)
            return _Rs([])
        raise AssertionError(f"未预期语句：{s}")


def _seed_ex(session: _ExFakeSession, ks: str, n_exp: int, n_meta: int) -> None:
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{}}")
    for i in range(n_exp):
        session.execute(
            f"INSERT INTO {ks}.experience_events (n) VALUES (%s)",
            (i, f"evt-{i}", "content", "actor", "acct", None, None, None),
        )
    for i in range(n_meta):
        session.execute(
            f"INSERT INTO {ks}.meta_events (node_key) VALUES (%s)",
            (f"nk-{i}", None, f"e-{i}", "reinforce", 1, None, "actor", "acct"),
        )


def test_ex_same_cluster_migration_full_steps():
    """本地档 EX 迁移全步骤真实执行：copy → 校验 → DROP 源 → 回拷正名 → DROP scratch。"""
    session = _ExFakeSession()
    _seed_ex(session, "ex_sp1", 3, 2)
    counts = migrate_mod._migrate_ex_same_cluster(session, "sp1")
    assert counts == (3, 2)
    assert len(session.tables["ex_sp1.experience_events"]) == 3  # 正名 keyspace 数据齐备
    assert len(session.tables["ex_sp1.meta_events"]) == 2
    assert not any(k.startswith("ex_sp1_mig.") for k in session.tables)  # scratch 已清理


def test_select_cell_exclude():
    """迁移选目标排除源 Cell（迁到自己不算迁移）。"""
    from lethefield_scheduler.watermark import NoOpenCellError, select_cell

    store = _FakeStore([])
    cell = select_cell(store, exclude=frozenset({"cell-1"}))
    assert cell.cell_id == "cell-2"
    with pytest.raises(NoOpenCellError):
        select_cell(store, exclude=frozenset({"cell-1", "cell-2"}))
