# This module is a copy-paste of test_lru_cache.py

from typing import Iterator
from unittest import mock

import pytest

from clean_python import ctx
from clean_python import SyncGateway
from clean_python import SyncLRUCache
from clean_python import Tenant


@pytest.fixture
def cache():
    gateway = mock.Mock(SyncGateway)
    cache = SyncLRUCache(gateway, max_size=3, multitenant=False)
    cache.gateway.get.return_value = {"some": "value"}
    cache.get("id")  # preseeds the cache
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
def cache_multitenant(tenant_context: Tenant):
    gateway = mock.Mock(SyncGateway)
    cache = SyncLRUCache(gateway, max_size=3, multitenant=True)
    cache.gateway.get.return_value = {"some": "value"}
    cache.get("id")  # preseeds the cache
    gateway.reset_mock()  # for assertions
    return cache


def test_get_cache_miss(cache: SyncLRUCache):
    cache.gateway.get.return_value = {"some": "other_value"}
    assert cache.get("id2") == {"some": "other_value"}
    cache.gateway.get.assert_called_once_with("id2")


def test_get_cache_hit(cache: SyncLRUCache):
    assert cache.get("id") == {"some": "value"}  # see fixture
    assert not cache.gateway.get.called


def test_get_cache_clear(cache: SyncLRUCache):
    cache.clear_cache()

    assert cache.get("id") == {"some": "value"}  # see fixture
    cache.gateway.get.assert_called_once_with("id")


def test_remove(cache: SyncLRUCache):
    cache.remove("id")
    cache.gateway.remove.assert_called_once_with("id")


def test_add(cache: SyncLRUCache):
    cache.add("item")
    cache.gateway.add.assert_called_once_with("item")


def test_filter(cache: SyncLRUCache):
    cache.filter(["filter"], "params")
    cache.gateway.filter.assert_called_once_with(["filter"], "params")


def test_get_multitenant(cache_multitenant: SyncLRUCache, tenant_context: Tenant):
    assert cache_multitenant.get("id") == {"some": "value"}  # see fixture
    assert not cache_multitenant.gateway.get.called


def test_get_other_tenant(
    cache_multitenant: SyncLRUCache, other_tenant_context: Tenant
):
    assert cache_multitenant.get("id") == {"some": "value"}  # see fixture
    # cache is missed because of different tenant
    cache_multitenant.gateway.get.assert_called_once_with("id")


def test_inplace_change_does_not_change_cache(cache: SyncLRUCache):
    cached = cache.get("id")
    cached["some"] = "other_value"
    assert cache.get("id") == {"some": "value"}


def test_inplace_nested_change_does_not_change_cache(cache: SyncLRUCache):
    cache.gateway.get.return_value = {"some": {"nested": "value"}}
    cached = cache.get("id2")
    cached["some"]["nested"] = "other_value"
    assert cache.get("id2") == {"some": {"nested": "value"}}
