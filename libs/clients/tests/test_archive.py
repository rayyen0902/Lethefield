"""archive 单测：表名常量、DDL/DML 生成、keyspace 校验、读写往返。"""

from datetime import UTC, datetime

import pytest
from lethefield_clients.archive import (
    ARCHIVE_TABLE,
    ensure_archive_table,
    list_archived,
    write_archive,
)


class FakeRow:
    def __init__(self, node_key: str, archived_at, snapshot: str) -> None:
        self.node_key = node_key
        self.archived_at = archived_at
        self.snapshot = snapshot


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple | None]] = []
        self.rows: list[FakeRow] = []

    def execute(self, query: str, params: tuple | None = None):
        self.calls.append((query, params))
        return self

    def all(self):
        return self.rows


def test_table_name_frozen():
    assert ARCHIVE_TABLE == "archived_nodes"


def test_ensure_archive_table_idempotent_ddl():
    session = FakeSession()
    ensure_archive_table(session, "m6_demo")
    query = session.calls[0][0]
    assert "CREATE TABLE IF NOT EXISTS m6_demo.archived_nodes" in query
    assert "node_key text PRIMARY KEY" in query


def test_write_archive_serializes_snapshot():
    session = FakeSession()
    when = datetime(2026, 8, 5, tzinfo=UTC)
    write_archive(
        session,
        "m6_demo",
        node_key="n1",
        snapshot={"props": {"s": 0.1}, "edges": []},
        archived_at=when,
    )
    query, params = session.calls[0]
    assert "INSERT INTO m6_demo.archived_nodes" in query
    assert params[0] == "n1"
    assert params[1] is when
    assert '"s": 0.1' in params[2]


def test_list_archived_decodes_snapshot():
    session = FakeSession()
    session.rows = [FakeRow("n1", None, '{"props": {"s": 0.1}, "edges": []}')]
    result = list_archived(session, "m6_demo")
    assert result[0]["node_key"] == "n1"
    assert result[0]["snapshot"]["props"]["s"] == 0.1


@pytest.mark.parametrize("bad", ["", "has-dash", "with space", "drop;table"])
def test_keyspace_validation_fail_closed(bad):
    session = FakeSession()
    with pytest.raises(ValueError, match="非法 keyspace"):
        ensure_archive_table(session, bad)
    assert session.calls == []
