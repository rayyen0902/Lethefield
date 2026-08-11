"""指标暴露口封装单测。"""

import urllib.request

from lethefield_metrics import metrics_port_from_env, start_metrics_server


def test_port_zero_disables():
    start_metrics_server(0)  # 不起服务，不报错


def test_server_serves_metrics():
    import socket

    from lethefield_metrics import counter
    from prometheus_client import REGISTRY

    probe = counter(
        "lethefield_metrics_exposition_probe_total",
        "暴露口自检探针（单测注册）",
        registry=REGISTRY,
    )
    probe.inc()

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    start_metrics_server(port)
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
        body = resp.read().decode()
    assert resp.status == 200
    assert "lethefield_metrics_exposition_probe_total" in body


def test_metrics_port_from_env(monkeypatch):
    assert metrics_port_from_env(9101) == 9101
    monkeypatch.setenv("LETHEFIELD_METRICS_PORT", "0")
    assert metrics_port_from_env(9101) == 0
