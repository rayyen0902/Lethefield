"""M13 红线 2：http_app 层 QuotaExceeded → 429 rate_limited 映射（message 含 quota_exceeded）。

最小化 HTTP 层测试：fake 依赖 + TestClient，service.record 被 monkeypatch 成抛
QuotaExceeded（当前无真实路径触发，写入链 M15 接线后此映射生效）。
"""

import time

import jwt
from fastapi.testclient import TestClient
from lethefield_api import service
from lethefield_api.http_app import create_app
from lethefield_clients import MappingCache, SpaceMapping, StaticControlPlaneStore
from lethefield_rms.quota import QuotaExceeded

SECRET = "test-secret"


class _FakeRedis:
    def incr(self, key):
        return 1

    def get(self, key):
        return None


class _FakeSession:
    def execute(self, query, params=None):
        raise AssertionError("不应触达 EX")


def _ctx() -> service.ApiContext:
    store = StaticControlPlaneStore.local()
    store.register_space(
        SpaceMapping(
            space_id="sp_1",
            cell_id="cell-local",
            ex_cluster_id="ex-local",
            pulsar_cluster_id="pulsar-local",
        )
    )
    return service.ApiContext(
        gremlin=None,
        es=None,
        ex_session=_FakeSession(),
        redis=_FakeRedis(),
        meta_appender=lambda **kw: None,
        mapping_cache=MappingCache(store),
    )


def _token() -> str:
    return jwt.encode(
        {
            "account_id": "acct-1",
            "space_id": ["sp_1"],
            "agent_actor_id": "claude-code",
            "scope": ["record"],
            "exp": int(time.time()) + 600,
        },
        SECRET,
        algorithm="HS256",
    )


def test_quota_exceeded_maps_to_429(monkeypatch):
    monkeypatch.setenv("LETHEFIELD_JWT_SECRET", SECRET)

    def _raise(*args, **kwargs):
        raise QuotaExceeded("vertex", "sp_1", 100, 100)

    monkeypatch.setattr(service, "record", _raise)
    client = TestClient(create_app(_ctx()))
    resp = client.post(
        "/memory/record",
        json={"space_id": "sp_1", "content": "x"},
        headers={"Authorization": f"Bearer {_token()}"},
    )
    assert resp.status_code == 429
    body = resp.json()["error"]
    assert body["code"] == "rate_limited"
    assert "quota_exceeded" in body["message"]
