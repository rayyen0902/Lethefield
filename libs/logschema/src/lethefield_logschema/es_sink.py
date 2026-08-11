"""es-ops 日志管线 shipper（M12）：LogEvent 异步批量进运维日志 ES 集群。

设计依据：开发文档 §13 / 设计文档 §19.5——space 粒度明细写结构化日志事件，
**异步批量**进 §12.1 的 ES 日志集群（es-ops，与 RMS 图索引物理隔离）。
本 shipper 是该管线的唯一写入端（单点）；与训练管线（含用户内容副本）
在埋点代码层面物理分开——这里只过运维/标定明细。

纪律：
- stderr 始终保留（docker logs 是最后一道兜底）；
- ES 不可达 / 队列满 → 事件不丢（stderr 已有），只降级不阻塞调用方；
- 一次性 CLI（decision_log submit 等短命进程）用 `emit(..., sync=True)` 直写，
  防进程退出丢队列尾部。
"""

import os
import queue
import sys
import threading
from contextlib import suppress
from datetime import UTC, datetime

from lethefield_logschema.events import LogEvent

_DEFAULT_URL = "http://localhost:9201"
_ENV_URL = "LETHEFIELD_OPS_ES_URL"
_INDEX_PREFIX = "lethefield-logs"


def _index_name(now: datetime | None = None) -> str:
    return f"{_INDEX_PREFIX}-{(now or datetime.now(UTC)).strftime('%Y.%m.%d')}"


class EsLogShipper:
    """后台线程批量 bulk 写 es-ops；fail-open（stderr 已留底，永不阻塞调用方）。"""

    def __init__(
        self,
        url: str,
        *,
        batch_size: int = 100,
        flush_interval: float = 2.0,
        max_queue: int = 10_000,
        client=None,
    ) -> None:
        if client is None:
            from elasticsearch import Elasticsearch

            client = Elasticsearch(url)
        self._client = client
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: queue.Queue[LogEvent] = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="es-log-shipper")
        self._thread.start()

    def submit(self, event: LogEvent) -> None:
        with suppress(queue.Full):
            # 队列满丢弃 ES 副本——stderr 兜底已留，不阻塞业务路径
            self._queue.put_nowait(event)

    def submit_sync(self, event: LogEvent) -> None:
        """一次性 CLI 用：同步直写（失败静默——stderr 兜底）。"""
        self._bulk([event])

    def _run(self) -> None:
        while not self._stop.is_set():
            batch = self._drain()
            if batch:
                self._bulk(batch)
            self._stop.wait(self._flush_interval)
        batch = self._drain()
        if batch:
            self._bulk(batch)

    def _drain(self) -> list[LogEvent]:
        batch = []
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _bulk(self, batch: list[LogEvent]) -> None:
        from elasticsearch.helpers import bulk

        with suppress(Exception):  # fail-open：stderr 已有副本
            bulk(
                self._client,
                (
                    {"_index": _index_name(e.timestamp), "_source": e.model_dump(mode="json")}
                    for e in batch
                ),
                raise_on_error=False,
            )

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)


_shipper: EsLogShipper | None = None
_lock = threading.Lock()


def configure(url: str | None = None, **kwargs) -> EsLogShipper:
    """配置进程级 shipper（幂等：重复调用返回既有实例）。常驻进程启动时调用一次。"""
    global _shipper
    with _lock:
        if _shipper is None:
            _shipper = EsLogShipper(url or os.environ.get(_ENV_URL, _DEFAULT_URL), **kwargs)
        return _shipper


def emit(event: LogEvent, *, sync: bool = False) -> None:
    """统一发射点：stderr 始终留底；已 configure 的进程顺带进 es-ops 管线。

    sync=True 供一次性 CLI（进程即将退出，异步入队会丢尾部）——惰性按默认/env
    配置建 shipper 并同步直写（ES 不可达静默降级，stderr 已有副本）。
    """
    print(event.to_jsonl(), file=sys.stderr)
    if sync:
        shipper = _shipper or configure()
        shipper.submit_sync(event)
        return
    shipper = _shipper
    if shipper is not None:
        shipper.submit(event)
