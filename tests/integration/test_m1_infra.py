"""M1 存储基础设施验收的集成测试。

覆盖开发文档 M1 四条验收标准：
1. 4 类存储物理隔离可证明（cluster_name/host_id/cluster_uuid 比对）
2. `ids.authority.wait-time` 保持默认值，巡检一键可验（红线 4）
3. 模拟时钟跳变触发告警（红线 6，注入 +60min 伪造样本模拟）
4.（重启基线为手动脚本 scripts/measure_restart_baseline.sh，不进 CI）
"""

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import check_graph_config  # noqa: E402
import verify_isolation  # noqa: E402
from lethefield_clock_monitor import check_offsets, collect_all  # noqa: E402
from lethefield_clock_monitor.check import OffsetSample  # noqa: E402

CFG_GRAPH = "it_m1_cfg"


def test_physical_isolation():
    assert verify_isolation.main() == 0


def test_ids_authority_wait_time_default(gremlin):
    # 静态：配置文件中不得显式配置该参数
    assert check_graph_config.static_check() == []
    # 运行时：动态图生效值必须等于默认值（先确保有图可开）
    gremlin.ensure_graph(CFG_GRAPH)
    assert check_graph_config.runtime_check() == []


def test_clock_offsets_within_threshold():
    samples = collect_all()
    assert check_offsets(samples) == []


def test_simulated_clock_jump_triggers_alert():
    """模拟某节点时钟跳变 +60 分钟（spike 实测场景为 +68min），必须触发告警。"""
    reference = datetime.now(UTC)
    jumped = OffsetSample(
        component="cassandra-cell(模拟跳变)",
        offset_seconds=3600.0,
        remote_time=reference + timedelta(minutes=60),
        reference_time=reference,
    )
    alerts = check_offsets([jumped])
    assert len(alerts) == 1
    assert "红线 6" in alerts[0]


def test_collect_failure_is_not_silent():
    """采集器失败（组件宕机）必须产生告警样本，不允许静默跳过——
    '系统看起来在正常运行'不构成时钟正常的证据。"""

    def dead_collector():
        raise ConnectionError("connection refused")

    samples = collect_all(collectors={"dead-component": dead_collector})
    alerts = check_offsets(samples)
    assert len(alerts) == 1
    assert "dead-component" in alerts[0]


def test_injected_slow_component_triggers_alert_via_collect():
    """经 collect_all 注入 +60min 伪造时钟源，端到端走采集-判定链路。"""
    samples = collect_all(collectors={"fake-es": lambda: datetime.now(UTC) + timedelta(minutes=60)})
    alerts = check_offsets(samples)
    assert len(alerts) == 1
    assert "fake-es" in alerts[0]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
