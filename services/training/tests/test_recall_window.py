"""R3 关联窗单测。"""

from lethefield_training.recall_window import RecallWindow

W = 60_000  # 60s 测试窗


def test_record_and_lookup(tmp_path):
    win = RecallWindow(tmp_path / "w.jsonl", w_r3_ms=W)
    win.record("ref_a", ["ev_1", "ev_2"], recalled_at_ms=1_000_000)
    assert win.recalled_at("ref_a", "ev_1", now_ms=1_000_000 + W) == 1_000_000
    assert win.recalled_at("ref_a", "ev_1", now_ms=1_000_000 + W + 1) is None  # 超窗
    assert win.recalled_at("ref_a", "ev_3", now_ms=1_000_000) is None  # 未召回
    assert win.recalled_at("ref_b", "ev_1", now_ms=1_000_000) is None  # 跨 space 不可见


def test_rebuild_from_file_and_prune(tmp_path):
    path = tmp_path / "w.jsonl"
    win = RecallWindow(path, w_r3_ms=W)
    now = RecallWindow._now_ms()
    win.record("ref_a", ["ev_fresh"], recalled_at_ms=now - 1000)
    win.record("ref_a", ["ev_stale"], recalled_at_ms=now - W - 1000)
    # 重启重建：超窗条目被 prune，窗内条目保留
    win2 = RecallWindow(path, w_r3_ms=W)
    assert win2.recalled_at("ref_a", "ev_fresh") is not None
    assert win2.recalled_at("ref_a", "ev_stale") is None


def test_latest_recall_wins(tmp_path):
    win = RecallWindow(tmp_path / "w.jsonl", w_r3_ms=W)
    win.record("ref_a", ["ev_1"], recalled_at_ms=1000)
    win.record("ref_a", ["ev_1"], recalled_at_ms=2000)
    assert win.recalled_at("ref_a", "ev_1", now_ms=2000) == 2000
