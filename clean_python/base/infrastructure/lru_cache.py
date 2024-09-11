from copy import deepcopy
from functools import lru_cache

from async_lru import alru_cache

from ..domain import ctx
from ..domain import Filter
from ..domain import Gateway
from ..domain import Id
from ..domain import Json
from ..domain import PageOptions
from ..domain import SyncGateway

__all__ = ["LRUCache", "SyncLRUCache"]


class LRUCache(Gateway):
    """This is a simple in-memory cache that uses the built in functools.lru_cache.

    The intended usage is to cache .get() calls. Because there is no cache invalidation
    it has rather strict constraints:

    - Data should be immutable.
    - A record should not be requested before it comes into existence.
    - If a record is deleted, it might linger in the cache.
    """

    def __init__(self, gateway: Gateway, max_size: int, multitenant: bool = False):
        self.gateway = gateway
        self.multitenant = multitenant
        self._cached_get = alru_cache(max_size)(self._get_with_tenant)

    async def _get_with_tenant(self, id: Id, tenant_id: Id | None) -> Json | None:
        # adds tenant_id to the arguments to have it in the cache key
        if tenant_id is not None:
            assert ctx.tenant
            assert ctx.tenant.id == tenant_id
        return await self.gateway.get(id)

    async def get(self, id: Id) -> Json | None:
        if self.multitenant:
            assert ctx.tenant
            result = await self._cached_get(id, ctx.tenant.id)
        else:
            result = await self._cached_get(id, None)
        # deepcopy to ensure the cache is not modified as a side effect
        # of the caller modifying the result
        return deepcopy(result)

    def clear_cache(self) -> None:
        self._cached_get.cache_clear()

    async def remove(self, id: Id) -> bool:
        # it's no use to clear the cache, because probably we will have
        # multiple processes running and the cache is not shared between them
        return await self.gateway.remove(id)

    async def add(self, item: Json) -> Json:
        # adding is allowed, it is only cached on first get()
        return await self.gateway.add(item)

    async def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        # filter bypasses the cache
        # TODO: in the special case of a filter for id, we could cache it
        return await self.gateway.filter(filters, params)


# This is a copy-paste of LRUCache, but with all the async / await removed:


class SyncLRUCache(SyncGateway):
    """This is a simple in-memory cache that uses the built in functools.lru_cache.

    The intended usage is to cache .get() calls. Because there is no cache invalidation
    it has rather strict constraints:

    - Data should be immutable.
    - A record should not be requested before it comes into existence.
    - If a record is deleted, it might linger in the cache.
    """

    def __init__(self, gateway: SyncGateway, max_size: int, multitenant: bool = False):
        self.gateway = gateway
        self.multitenant = multitenant
        self._cached_get = lru_cache(max_size)(self._get_with_tenant)

    def _get_with_tenant(self, id: Id, tenant_id: Id | None) -> Json | None:
        # adds tenant_id to the arguments to have it in the cache key
        if tenant_id is not None:
            assert ctx.tenant
            assert ctx.tenant.id == tenant_id
        return self.gateway.get(id)

    def get(self, id: Id) -> Json | None:
        if self.multitenant:
            assert ctx.tenant
            result = self._cached_get(id, ctx.tenant.id)
        else:
            result = self._cached_get(id, None)
        # deepcopy to ensure the cache is not modified as a side effect
        # of the caller modifying the result
        return deepcopy(result)

    def clear_cache(self) -> None:
        self._cached_get.cache_clear()

    def remove(self, id: Id) -> bool:
        # it's no use to clear the cache, because probably we will have
        # multiple processes running and the cache is not shared between them
        return self.gateway.remove(id)

    def add(self, item: Json) -> Json:
        # adding is allowed, it is only cached on first get()
        return self.gateway.add(item)

    def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        # filter bypasses the cache
        # TODO: in the special case of a filter for id, we could cache it
        return self.gateway.filter(filters, params)
