from typing import Iterator
from unittest import mock

import pytest

from clean_python import ctx
from clean_python import Gateway
from clean_python import LRUCache
from clean_python import Tenant


@pytest.fixture
async def cache():
    gateway = mock.Mock(Gateway)
    cache = LRUCache(gateway, max_size=3, multitenant=False)
    cache.gateway.get.return_value = {"some": "value"}
    await cache.get("id")  # preseeds the cache
    gateway.reset_mock()  # for assertions
    return cache


@pytest.fixture
def tenant_context() -> Iterator[Tenant]:
    old_tenant = ctx.tenant
    ctx.tenant = Tenant(id="tenant_id", name="")
    yield ctx.tenant
    ctx.tenant = old_tenant


@pytest.fixture
def other_tenant_context() -> Iterator[Tenant]:
    old_tenant = ctx.tenant
    ctx.tenant = Tenant(id="tenant_id2", name="")
    yield ctx.tenant
    ctx.tenant = old_tenant


@pytest.fixture
async def cache_multitenant(tenant_context: Tenant):
    gateway = mock.Mock(Gateway)
    cache = LRUCache(gateway, max_size=3, multitenant=True)
    cache.gateway.get.return_value = {"some": "value"}
    await cache.get("id")  # preseeds the cache
    gateway.reset_mock()  # for assertions
    return cache


async def test_get_cache_miss(cache: LRUCache):
    cache.gateway.get.return_value = {"some": "other_value"}
    assert await cache.get("id2") == {"some": "other_value"}
    cache.gateway.get.assert_awaited_once_with("id2")


async def test_get_cache_hit(cache: LRUCache):
    assert await cache.get("id") == {"some": "value"}  # see fixture
    assert not cache.gateway.get.called


async def test_get_cache_clear(cache: LRUCache):
    cache.clear_cache()

    assert await cache.get("id") == {"some": "value"}  # see fixture
    cache.gateway.get.assert_awaited_once_with("id")


async def test_remove(cache: LRUCache):
    await cache.remove("id")
    cache.gateway.remove.assert_awaited_once_with("id")


async def test_add(cache: LRUCache):
    await cache.add("item")
    cache.gateway.add.assert_awaited_once_with("item")


async def test_filter(cache: LRUCache):
    await cache.filter(["filter"], "params")
    cache.gateway.filter.assert_awaited_once_with(["filter"], "params")


async def test_get_multitenant(cache_multitenant: LRUCache, tenant_context: Tenant):
    assert await cache_multitenant.get("id") == {"some": "value"}  # see fixture
    assert not cache_multitenant.gateway.get.called


async def test_get_other_tenant(
    cache_multitenant: LRUCache, other_tenant_context: Tenant
):
    assert await cache_multitenant.get("id") == {"some": "value"}  # see fixture
    # cache is missed because of different tenant
    cache_multitenant.gateway.get.assert_awaited_once_with("id")


async def test_inplace_change_does_not_change_cache(cache: LRUCache):
    cached = await cache.get("id")
    cached["some"] = "other_value"
    assert await cache.get("id") == {"some": "value"}


async def test_inplace_nested_change_does_not_change_cache(cache: LRUCache):
    cache.gateway.get.return_value = {"some": {"nested": "value"}}
    cached = await cache.get("id2")
    cached["some"]["nested"] = "other_value"
    assert await cache.get("id2") == {"some": {"nested": "value"}}
