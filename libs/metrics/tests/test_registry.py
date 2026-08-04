import pytest
from lethefield_metrics import counter, gauge, histogram
from prometheus_client import CollectorRegistry


@pytest.fixture
def registry():
    return CollectorRegistry()


def test_valid_names_accepted(registry):
    counter("lethefield_ff_delta_applied_total", "δ 触发计数", registry=registry)
    gauge("lethefield_ff_theta_filter_ratio", "θ 过滤比例", registry=registry)
    histogram("lethefield_record_confirm_duration_seconds", "确认延迟", registry=registry)


@pytest.mark.parametrize(
    "bad_name",
    [
        "ff_delta_applied_total",  # 缺 lethefield_ 前缀
        "lethefield_ff",  # 只有一段，缺域/单位
        "lethefield_ff_delta_applied_widgets",  # 非法单位后缀
        "lethefield_FF_delta_total",  # 大写
    ],
)
def test_invalid_names_rejected(bad_name, registry):
    with pytest.raises(ValueError, match="命名规则|单位后缀"):
        counter(bad_name, "x", registry=registry)


def test_blacklisted_labels_rejected(registry):
    with pytest.raises(ValueError, match="黑名单"):
        counter(
            "lethefield_ex_write_total",
            "x",
            labels=["space_id"],
            registry=registry,
        )
    with pytest.raises(ValueError, match="黑名单"):
        gauge(
            "lethefield_ff_theta_filter_ratio",
            "x",
            labels=["node_key"],
            registry=registry,
        )


def test_unknown_label_rejected(registry):
    with pytest.raises(ValueError, match="白名单"):
        counter(
            "lethefield_ex_write_total",
            "x",
            labels=["datacenter"],
            registry=registry,
        )


def test_whitelisted_labels_accepted_and_usable(registry):
    c = counter(
        "lethefield_agent_suggestion_total",
        "x",
        labels=["service", "outcome"],
        registry=registry,
    )
    c.labels(service="ops", outcome="accepted").inc()
    value = registry.get_sample_value(
        "lethefield_agent_suggestion_total",
        {"service": "ops", "outcome": "accepted"},
    )
    assert value == 1.0
