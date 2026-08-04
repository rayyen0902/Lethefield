"""连接工厂的参数解析测试——不连真实服务（连通性由集成测试与 wait_for_stack.sh 覆盖）。"""

import pytest
from lethefield_clients.factories import cassandra_cluster, parse_hosts


def test_parse_hosts_basic():
    assert parse_hosts("a,b, c ,,") == ["a", "b", "c"]


def test_parse_hosts_empty():
    assert parse_hosts("") == []


def test_cassandra_cluster_uses_parsed_hosts():
    cluster = cassandra_cluster(hosts="localhost,127.0.0.1", port=9999)
    assert cluster.contact_points == ["localhost", "127.0.0.1"]
    assert cluster.port == 9999


def test_cassandra_cluster_env_override(monkeypatch):
    monkeypatch.setenv("LETHEFIELD_CASSANDRA_HOSTS", "localhost")
    monkeypatch.setenv("LETHEFIELD_CASSANDRA_PORT", "9142")
    cluster = cassandra_cluster()
    assert cluster.contact_points == ["localhost"]
    assert cluster.port == 9142


def test_missing_env_falls_back_to_localhost(monkeypatch):
    monkeypatch.delenv("LETHEFIELD_CASSANDRA_HOSTS", raising=False)
    with pytest.MonkeyPatch.context() as m:
        m.delenv("LETHEFIELD_CASSANDRA_PORT", raising=False)
        cluster = cassandra_cluster()
    assert cluster.contact_points == ["localhost"]
