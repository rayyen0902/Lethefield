"""计算侧映射缓存（M9）："调度器宕机不影响存量读写"的实现点。

设计文档 §17.2 硬性验收：存量读写不经过调度器——计算池持映射缓存直连 Cell，
控制面故障只影响开通/迁移/注销。本模块是计算侧（API/FS 等）的统一缓存层：

- TTL 缓存 `get_space_mapping` / `list_spaces` 结果（含"不存在"负缓存）；
- **控制面异常时服务陈旧值**（fail-open to cache）：有陈旧条目即用，没有才上抛；
- `SpaceNotFoundError` 是业务语义（space 未注册）而非控制面故障——正常上抛并负缓存。

时钟可注入（单测用）；生产用 monotonic。
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

from lethefield_clients.control_plane import ControlPlaneStore, SpaceMapping, SpaceNotFoundError

# 负缓存哨兵（space 已确认未注册）
_NOT_FOUND = object()


@dataclass
class _Entry:
    value: object
    expires_at: float


class MappingCache:
    """ControlPlaneStore 的 TTL 缓存包装；控制面故障时服务陈旧值。

    只包装计算侧读取路径（get_space_mapping/list_spaces）；写操作
    （register/update/unregister）永远直达 store——调度器操作不走缓存。
    """

    def __init__(
        self,
        store: ControlPlaneStore,
        ttl_seconds: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._ttl = ttl_seconds
        self._clock = clock
        self._mappings: dict[str, _Entry] = {}
        self._space_list: _Entry | None = None

    @property
    def store(self) -> ControlPlaneStore:
        return self._store

    def get_space_mapping(self, space_id: str) -> SpaceMapping:
        entry = self._mappings.get(space_id)
        if entry is not None and entry.expires_at > self._clock():
            return self._unwrap(space_id, entry.value)
        try:
            mapping = self._store.get_space_mapping(space_id)
        except SpaceNotFoundError:
            self._mappings[space_id] = _Entry(_NOT_FOUND, self._clock() + self._ttl)
            raise
        except Exception:
            if entry is not None:  # 控制面故障：陈旧值续命（不刷新 TTL，故障期一直可用）
                return self._unwrap(space_id, entry.value)
            raise
        self._mappings[space_id] = _Entry(mapping, self._clock() + self._ttl)
        return mapping

    def list_spaces(self) -> list[str]:
        if self._space_list is not None and self._space_list.expires_at > self._clock():
            return list(self._space_list.value)
        try:
            spaces = self._store.list_spaces()
        except Exception:
            if self._space_list is not None:
                return list(self._space_list.value)
            raise
        self._space_list = _Entry(spaces, self._clock() + self._ttl)
        return list(spaces)

    def invalidate(self, space_id: str | None = None) -> None:
        """注销/迁移等控制面写操作后主动失效（space_id=None 全清）。"""
        if space_id is None:
            self._mappings.clear()
        else:
            self._mappings.pop(space_id, None)
        self._space_list = None

    @staticmethod
    def _unwrap(space_id: str, value: object) -> SpaceMapping:
        if value is _NOT_FOUND:
            raise SpaceNotFoundError(space_id)
        return value  # type: ignore[return-value]
