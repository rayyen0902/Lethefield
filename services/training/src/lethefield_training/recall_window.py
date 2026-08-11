"""R3 关联窗（召回明细 × 纠错对，node_key + W_r3 时间窗命中判定）。

召回明细过境期间落 `recall_window.jsonl`（worker 重启可重建窗口）；
启动加载时 prune 超窗条目，窗口随查询惰性过期。未命中明细随 topic retention
滚动清除（过境 ≠ 沉淀），本文件只是关联状态的续命载体，不是样本沉淀。
"""

import json
from datetime import UTC, datetime
from pathlib import Path


class RecallWindow:
    """(space_ref, node_key, recalled_at_ms) 的有界窗口；W_r3 来自 TrainingConfig。

    另持 event_id 去重集（M12 ③ 收口定案：过滤器 at-least-once，worker 按 ID 去重，
    否则重发虚增 R3 关联基数）；随窗口同一时间界 prune。
    """

    def __init__(self, path: str | Path, *, w_r3_ms: int) -> None:
        self._path = Path(path)
        self._ids_path = self._path.with_name("recall_seen_ids.jsonl")
        self._w_r3_ms = w_r3_ms
        # (space_ref, node_key) -> 最近一次召回时间（epoch ms）
        self._seen: dict[tuple[str, str], int] = {}
        self._seen_ids: set[str] = set()
        self._load()

    @staticmethod
    def _now_ms() -> int:
        return int(datetime.now(UTC).timestamp() * 1000)

    def _load(self) -> None:
        now = self._now_ms()
        if self._path.exists():
            with self._path.open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    recalled_at = int(obj["recalled_at_ms"])
                    if now - recalled_at <= self._w_r3_ms:
                        key = (obj["space_ref"], obj["node_key"])
                        self._seen[key] = max(recalled_at, self._seen.get(key, 0))
        if self._ids_path.exists():
            with self._ids_path.open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    if now - int(obj["at_ms"]) <= self._w_r3_ms:
                        self._seen_ids.add(obj["event_id"])

    def mark_seen(self, event_id: str, *, at_ms: int | None = None) -> bool:
        """登记 event_id；已见过返回 False（调用方跳过重放）。"""
        if event_id in self._seen_ids:
            return False
        at_ms = at_ms if at_ms is not None else self._now_ms()
        self._seen_ids.add(event_id)
        self._ids_path.parent.mkdir(parents=True, exist_ok=True)
        with self._ids_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"event_id": event_id, "at_ms": at_ms}, sort_keys=True) + "\n")
        return True

    def record(self, space_ref: str, node_keys: list[str], *, recalled_at_ms: int) -> None:
        """登记一次召回明细（覆盖同键更早时间戳——关联只看最近召回）。"""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            for node_key in node_keys:
                self._seen[(space_ref, node_key)] = recalled_at_ms
                f.write(
                    json.dumps(
                        {
                            "space_ref": space_ref,
                            "node_key": node_key,
                            "recalled_at_ms": recalled_at_ms,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    + "\n"
                )

    def recalled_at(
        self, space_ref: str, node_key: str, *, now_ms: int | None = None
    ) -> int | None:
        """查 (space_ref, node_key) 在 W_r3 窗内的最近召回时间；未命中/超窗返回 None。"""
        now_ms = now_ms if now_ms is not None else self._now_ms()
        recalled_at = self._seen.get((space_ref, node_key))
        if recalled_at is None or now_ms - recalled_at > self._w_r3_ms:
            return None
        return recalled_at
