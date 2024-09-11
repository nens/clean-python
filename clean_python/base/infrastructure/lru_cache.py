from functools import lru_cache

from ..domain import ctx
from ..domain import Filter
from ..domain import Id
from ..domain import Json
from ..domain import PageOptions
from ..domain import SyncGateway

__all__ = ["SyncLRUCache"]


class SyncLRUCache(SyncGateway):
    """This is a simple in-memory cache that uses the built in functools.lru_cache.

    The intended usage is to cache .get() calls for immutable data. Note that if
    the actual record is deleted, it might be still in the cache.
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

    def _get(self, id: Id) -> Json | None:
        return self.gateway.get(id)

    def get(self, id: Id) -> Json | None:
        if self.multitenant:
            assert ctx.tenant
            return self._cached_get(id, ctx.tenant.id)
        else:
            return self._cached_get(id, None)

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
