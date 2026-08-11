"""es-ops shipper 单测（fake client，不起栈）。"""

import time

from lethefield_logschema import LogEvent, es_sink


class FakeEsClient:
    def __init__(self):
        self.bulk_calls: list[list[dict]] = []

    def bulk(self, actions, **kwargs):  # 经 elasticsearch.helpers.bulk 调用
        self.bulk_calls.append(list(actions))
        return (len(self.bulk_calls[-1]), [])


def _event(marker: str) -> LogEvent:
    return LogEvent(service="test", event_type="test_event", payload={"m": marker})


def test_shipper_batches_and_flushes(monkeypatch):
    import elasticsearch.helpers

    client = FakeEsClient()
    monkeypatch.setattr(
        elasticsearch.helpers, "bulk", lambda c, actions, **kw: client.bulk(actions, **kw)
    )
    shipper = es_sink.EsLogShipper(
        "http://fake:9201", batch_size=2, flush_interval=0.05, client=client
    )
    try:
        for i in range(3):
            shipper.submit(_event(str(i)))
        deadline = time.time() + 5
        while time.time() < deadline and sum(len(c) for c in client.bulk_calls) < 3:
            time.sleep(0.05)
        docs = [a for c in client.bulk_calls for a in c]
        assert len(docs) == 3
        assert all(d["_index"].startswith("lethefield-logs-") for d in docs)
        assert docs[0]["_source"]["payload"] == {"m": "0"}
    finally:
        shipper.close()


def test_shipper_fail_open_on_es_error(monkeypatch, capsys):
    import elasticsearch.helpers

    def boom(client, actions, **kw):
        raise ConnectionError("es down")

    monkeypatch.setattr(elasticsearch.helpers, "bulk", boom)
    shipper = es_sink.EsLogShipper("http://fake:9201", flush_interval=0.05)
    try:
        shipper.submit_sync(_event("x"))  # 不抛——stderr 兜底
    finally:
        shipper.close()


def test_emit_without_configure_prints_only(capsys):
    es_sink._shipper = None  # 未配置：只 stderr，不报错
    es_sink.emit(_event("plain"))
    assert "test_event" in capsys.readouterr().err


def test_emit_sync_lazy_configures(monkeypatch):
    import elasticsearch.helpers

    client = FakeEsClient()
    monkeypatch.setattr(
        elasticsearch.helpers, "bulk", lambda c, actions, **kw: client.bulk(actions, **kw)
    )
    monkeypatch.setattr(es_sink, "_shipper", None)

    class FakeShipper(es_sink.EsLogShipper):
        def __init__(self, url, **kw):
            super().__init__(url, client=client, **kw)

    monkeypatch.setattr(es_sink, "EsLogShipper", FakeShipper)
    es_sink.emit(_event("sync"), sync=True)
    assert sum(len(c) for c in client.bulk_calls) == 1
    es_sink._shipper.close()
    es_sink._shipper = None
