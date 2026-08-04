"""带强制规则的指标工厂。

命名与标签规则在注册时校验，不符直接抛 ValueError——
规则不依赖 code review 记忆，由代码层拒绝非法注册。
"""

import re
from collections.abc import Sequence

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# 允许的单位后缀（OpenMetrics 语义）。开发文档 §19 指标清单用到的单位已全部覆盖；
# 新增单位意味着扩这个集合本身——刻意做成一次需要评审的代码改动，而不是随意放过。
UNIT_SUFFIXES: frozenset[str] = frozenset({"seconds", "bytes", "ratio", "rate", "total", "events"})

# 标签白名单：低基数枚举类（§19.5）。space 粒度明细走日志管线，不进指标标签。
LABEL_WHITELIST: frozenset[str] = frozenset(
    {
        "service",
        "instance",
        "cell_id",
        "tier",
        "type",
        "stage",
        "result",
        "dimension",
        "bound",
        "namespace_class",
        "outcome",
        "reason",
    }
)

# 标签黑名单：高基数 / 违反红线 1。即使将来扩白名单也不允许这几个。
LABEL_BLACKLIST: frozenset[str] = frozenset({"space_id", "node_key"})

_NAME_RE = re.compile(r"^lethefield_[a-z0-9]+(?:_[a-z0-9]+)*_([a-z0-9]+)$")


def _validate_name(name: str) -> None:
    match = _NAME_RE.match(name)
    if match is None:
        raise ValueError(f"指标名 {name!r} 不符合 lethefield_<域>_<名称>_<单位> 命名规则")
    unit = match.group(1)
    if unit not in UNIT_SUFFIXES:
        raise ValueError(
            f"指标名 {name!r} 的单位后缀 {unit!r} 不在允许集合 {sorted(UNIT_SUFFIXES)} 中"
        )


def _validate_labels(labels: Sequence[str]) -> None:
    for label in labels:
        if label in LABEL_BLACKLIST:
            raise ValueError(f"标签 {label!r} 在黑名单中（防基数爆炸 + 守红线 1）")
        if label not in LABEL_WHITELIST:
            raise ValueError(f"标签 {label!r} 不在白名单中")


def counter(
    name: str,
    description: str,
    labels: Sequence[str] = (),
    registry: CollectorRegistry | None = None,
) -> Counter:
    _validate_name(name)
    _validate_labels(labels)
    return Counter(name, description, labels, registry=registry)


def gauge(
    name: str,
    description: str,
    labels: Sequence[str] = (),
    registry: CollectorRegistry | None = None,
) -> Gauge:
    _validate_name(name)
    _validate_labels(labels)
    return Gauge(name, description, labels, registry=registry)


def histogram(
    name: str,
    description: str,
    labels: Sequence[str] = (),
    registry: CollectorRegistry | None = None,
    **kwargs,
) -> Histogram:
    _validate_name(name)
    _validate_labels(labels)
    return Histogram(name, description, labels, registry=registry, **kwargs)
