"""热层落盘 + 清单索引（M11 可撤回性的核心机制，1.0 内建）。

目录布局（root = TrainingConfig.hot_root）：
    hot/samples-YYYY-MM-DD.jsonl   样本本体（按日落盘，JSONL 追加）
    index/{space_ref}.jsonl        每 space_ref 的样本清单（{sample_id, file, rule,
                                   source, created_at, scrubbed}）；space_ref=None 的
                                   ①② 类样本归 `_ops` 清单。

可定位性（§12.4 硬要求）：撤回/销毁处置只读目标 space_ref 的清单文件，
按清单直接找到样本所在文件重写——定位是 O(清单) 操作，不是全量扫描。
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from lethefield_training.sample import TrainingSample

# space_ref 为 None 的运维元数据样本的清单名（①② 类）
OPS_MANIFEST = "_ops"


@dataclass(frozen=True)
class ManifestEntry:
    sample_id: str
    file: str  # 相对 root 的路径
    rule: str
    source: str
    created_at: str
    scrubbed: bool = False

    def to_json(self) -> str:
        return json.dumps(self.__dict__, ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> "ManifestEntry":
        return cls(**json.loads(data))


class HotSampleStore:
    """热层样本存取：追加落盘 + 清单索引 + 成组处置（scrub）。"""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def _manifest_path(self, space_ref: str | None) -> Path:
        return self._root / "index" / f"{space_ref or OPS_MANIFEST}.jsonl"

    def append(self, samples: list[TrainingSample], *, now: datetime | None = None) -> None:
        """批次落盘：样本写当日 hot 文件 + 清单追加（先样本后清单，清单即存在性证明）。"""
        if not samples:
            return
        day = (now or datetime.now(UTC)).strftime("%Y-%m-%d")
        rel = f"hot/samples-{day}.jsonl"
        path = self._root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            for s in samples:
                f.write(s.to_json() + "\n")
        manifests: dict[str | None, list[ManifestEntry]] = {}
        for s in samples:
            manifests.setdefault(s.space_ref, []).append(
                ManifestEntry(
                    sample_id=s.sample_id,
                    file=rel,
                    rule=s.rule,
                    source=s.source,
                    created_at=s.created_at.isoformat(),
                    scrubbed=s.scrubbed,
                )
            )
        for space_ref, entries in manifests.items():
            mpath = self._manifest_path(space_ref)
            mpath.parent.mkdir(parents=True, exist_ok=True)
            with mpath.open("a", encoding="utf-8") as f:
                for e in entries:
                    f.write(e.to_json() + "\n")

    def manifest(self, space_ref: str | None) -> list[ManifestEntry]:
        """读某 space_ref 的样本清单（定位入口，O(清单)——不触碰其他清单/样本文件）。"""
        path = self._manifest_path(space_ref)
        if not path.exists():
            return []
        with path.open(encoding="utf-8") as f:
            return [ManifestEntry.from_json(line) for line in f if line.strip()]

    def load_sample(self, rel_file: str, sample_id: str) -> TrainingSample | None:
        path = self._root / rel_file
        if not path.exists():
            return None
        with path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                sample = TrainingSample.from_json(line)
                if sample.sample_id == sample_id:
                    return sample
        return None

    def scrub(self, space_ref: str) -> int:
        """撤回/销毁处置：该 space_ref 全部存量样本内容字段清除、骨架保留。

        只读目标 space_ref 的清单 → 按清单定位文件重写（O(清单)）；
        清单同步标记 scrubbed。幂等：已 scrubbed 的条目跳过。返回处置条数。
        """
        entries = self.manifest(space_ref)
        targets = [e for e in entries if not e.scrubbed]
        if not targets:
            return 0
        by_file: dict[str, set[str]] = {}
        for e in targets:
            by_file.setdefault(e.file, set()).add(e.sample_id)
        for rel, ids in by_file.items():
            path = self._root / rel
            if not path.exists():
                continue
            lines = []
            with path.open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    sample = TrainingSample.from_json(line)
                    if sample.sample_id in ids:
                        sample = sample.scrubbed_copy()
                    lines.append(sample.to_json())
            with path.open("w", encoding="utf-8") as f:
                for line in lines:
                    f.write(line + "\n")
        # 清单重写：目标条目标记 scrubbed（清单追加 scrub 记录语义 = 重写后的状态行）
        remaining = [
            ManifestEntry(
                sample_id=e.sample_id,
                file=e.file,
                rule=e.rule,
                source=e.source,
                created_at=e.created_at,
                scrubbed=True,
            )
            if e in targets
            else e
            for e in entries
        ]
        mpath = self._manifest_path(space_ref)
        with mpath.open("w", encoding="utf-8") as f:
            for e in remaining:
                f.write(e.to_json() + "\n")
        return len(targets)

    def prune_hot(self, *, retention_days: int, now: datetime | None = None) -> int:
        """热层滚动清理：删除 retention_days 之前的 samples-*.jsonl（按文件名日期）。"""
        now = now or datetime.now(UTC)
        hot = self._root / "hot"
        if not hot.exists():
            return 0
        removed = 0
        for path in hot.glob("samples-*.jsonl"):
            day = path.stem.removeprefix("samples-")
            try:
                file_date = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError:
                continue
            if (now - file_date).days > retention_days:
                path.unlink()
                removed += 1
        return removed
