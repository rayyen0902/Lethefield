"""指标暴露口薄封装（M12）。

常驻进程（fs/training/ingest_dms/metrics_exporter 的 loop 形态）在启动时调用
`start_metrics_server` 起 prometheus_client 自带 HTTP 暴露口；一次性 CLI 不起
（其聚合类计数统一走"LogEvent → metrics_exporter 聚合"通道，进程内 counter
对短命进程无意义——M12 定案）。

端口约定（env 可覆盖）：fs 9101 / training 9102 / ingest_dms 9103 /
metrics_exporter 9104；API 走自身应用端口的 /metrics 端点，不用本封装。
"""

import os

from prometheus_client import start_http_server

_ENV_PORT = "LETHEFIELD_METRICS_PORT"


def metrics_port_from_env(default: int) -> int:
    """读 LETHEFIELD_METRICS_PORT；0 = 不起暴露口（默认各服务给定值）。"""
    return int(os.environ.get(_ENV_PORT, default))


def start_metrics_server(port: int) -> None:
    """起 /metrics HTTP 暴露口（后台 daemon 线程）；port<=0 时不起。"""
    if port <= 0:
        return
    start_http_server(port)
