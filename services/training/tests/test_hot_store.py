"""热层落盘 + 清单索引 + scrub 处置单测。"""

from datetime import UTC, datetime, timedelta

from lethefield_training.hot_store import OPS_MANIFEST, HotSampleStore
from lethefield_training.sample import TrainingSample


def _sample(space_ref, rule="R3", source="ex_derived", content="x"):
    return TrainingSample.new(
        source=source,
        rule=rule,
        space_ref=space_ref,
        problem={"c": content},
        diagnosis={},
        decision={},
        outcome={},
        auth_scope="granted" if space_ref else "ops_only",
    )


def test_append_and_manifest(tmp_path):
    store = HotSampleStore(tmp_path)
    s1, s2 = _sample("ref_a"), _sample(None, rule="R1", source="decision_log")
    store.append([s1, s2])
    manifest_a = store.manifest("ref_a")
    assert [e.sample_id for e in manifest_a] == [s1.sample_id]
    manifest_ops = store.manifest(OPS_MANIFEST)
    assert [e.sample_id for e in manifest_ops] == [s2.sample_id]
    assert store.load_sample(manifest_a[0].file, s1.sample_id) == s1


def test_manifest_missing_returns_empty(tmp_path):
    assert HotSampleStore(tmp_path).manifest("nobody") == []


def test_scrub_locates_via_manifest_and_clears_content(tmp_path):
    store = HotSampleStore(tmp_path)
    target = [_sample("ref_a", content=f"a{i}") for i in range(3)]
    other = _sample("ref_b", content="keep-me")
    store.append([*target, other])
    scrubbed = store.scrub("ref_a")
    assert scrubbed == 3
    for e in store.manifest("ref_a"):
        assert e.scrubbed is True
        sample = store.load_sample(e.file, e.sample_id)
        assert sample.scrubbed is True
        assert sample.problem == {}
    # 其他 space 样本不受影响（同日落盘同文件也保持原样）
    other_entry = store.manifest("ref_b")[0]
    assert other_entry.scrubbed is False
    assert store.load_sample(other_entry.file, other.sample_id).problem == {"c": "keep-me"}


def test_scrub_idempotent(tmp_path):
    store = HotSampleStore(tmp_path)
    store.append([_sample("ref_a")])
    assert store.scrub("ref_a") == 1
    assert store.scrub("ref_a") == 0  # 已 scrubbed 条目跳过


def test_prune_hot(tmp_path):
    store = HotSampleStore(tmp_path)
    store.append([_sample("ref_a")], now=datetime.now(UTC))
    old_day = (datetime.now(UTC) - timedelta(days=100)).strftime("%Y-%m-%d")
    old_file = tmp_path / "hot" / f"samples-{old_day}.jsonl"
    old_file.write_text(_sample("ref_a").to_json() + "\n", encoding="utf-8")
    assert store.prune_hot(retention_days=90) == 1
    assert not old_file.exists()
    assert store.prune_hot(retention_days=90) == 0
