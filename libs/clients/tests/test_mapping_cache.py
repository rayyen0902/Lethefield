import pytest
from lethefield_clients import (
    MappingCache,
    SpaceMapping,
    SpaceNotFoundError,
    StaticControlPlaneStore,
    Tier,
)


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _mapping(space_id: str) -> SpaceMapping:
    return SpaceMapping(
        space_id=space_id,
        cell_id="cell-local",
        ex_cluster_id="ex-local",
        pulsar_cluster_id="pulsar-local",
        tier=Tier.COLD,
    )


@pytest.fixture
def store():
    s = StaticControlPlaneStore.local()
    s.register_space(_mapping("alpha"))
    return s


def test_cache_hit_avoids_store(store):
    cache = MappingCache(store, ttl_seconds=100)
    assert cache.get_space_mapping("alpha").cell_id == "cell-local"
    store._spaces.clear()  # 第二次命中缓存，不触 store
    assert cache.get_space_mapping("alpha").cell_id == "cell-local"


def test_ttl_expiry_refreshes(store):
    clock = _Clock()
    cache = MappingCache(store, ttl_seconds=10, clock=clock)
    cache.get_space_mapping("alpha")
    store._spaces["alpha"] = SpaceMapping(
        space_id="alpha",
        cell_id="cell-local",
        ex_cluster_id="ex-local",
        pulsar_cluster_id="pulsar-local",
        tier=Tier.HOT,
    )
    clock.t = 11  # 过 TTL
    assert cache.get_space_mapping("alpha").tier == Tier.HOT


def test_not_found_negative_cached(store):
    cache = MappingCache(store, ttl_seconds=100)
    with pytest.raises(SpaceNotFoundError):
        cache.get_space_mapping("ghost")
    # 负缓存期间注册了新映射也不可见（TTL 内）
    store.register_space(_mapping("ghost"))
    with pytest.raises(SpaceNotFoundError):
        cache.get_space_mapping("ghost")


class _BrokenStore:
    def __init__(self) -> None:
        self.calls = 0

    def get_space_mapping(self, space_id):
        self.calls += 1
        raise ConnectionError("control plane down")

    def list_spaces(self):
        self.calls += 1
        raise ConnectionError("control plane down")


def test_store_outage_serves_stale_mapping(store):
    """M9 硬性验收的实现语义：控制面故障时陈旧缓存继续服务存量读写。"""
    clock = _Clock()
    cache = MappingCache(store, ttl_seconds=10, clock=clock)
    cache.get_space_mapping("alpha")
    cache.list_spaces()
    cache._store = _BrokenStore()  # 调度器/控制面整体下线
    clock.t = 100  # 陈旧也服务
    assert cache.get_space_mapping("alpha").cell_id == "cell-local"
    assert cache.list_spaces() == ["alpha"]
    with pytest.raises(ConnectionError):  # 缓存未覆盖的 space 无陈旧值可服务
        cache.get_space_mapping("beta")


def test_stale_negative_entry_reraises_not_found(store):
    clock = _Clock()
    cache = MappingCache(store, ttl_seconds=10, clock=clock)
    with pytest.raises(SpaceNotFoundError):
        cache.get_space_mapping("ghost")
    cache._store = _BrokenStore()
    clock.t = 100
    with pytest.raises(SpaceNotFoundError):  # 陈旧负缓存仍是 404 语义
        cache.get_space_mapping("ghost")


def test_invalidate(store):
    cache = MappingCache(store, ttl_seconds=100)
    cache.get_space_mapping("alpha")
    cache.list_spaces()
    cache.invalidate("alpha")
    store._spaces.clear()
    with pytest.raises(SpaceNotFoundError):
        cache.get_space_mapping("alpha")
    assert cache.list_spaces() == []
