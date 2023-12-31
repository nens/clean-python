# This module is a copy paste of test_int_api_gateway.py

import pytest

from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Json
from clean_python import Tenant
from clean_python.api_client import SyncApiGateway
from clean_python.api_client import SyncApiProvider


class BooksGateway(SyncApiGateway, path="v1/books/{id}"):
    pass


@pytest.fixture
def provider(fastapi_example_app) -> SyncApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield SyncApiProvider(
        fastapi_example_app + "/", lambda: {"Authorization": "Bearer token"}
    )
    ctx.tenant = None


@pytest.fixture
def gateway(provider) -> SyncApiGateway:
    return BooksGateway(provider)


@pytest.fixture
def book(gateway: SyncApiGateway):
    return gateway.add({"title": "fixture", "author": {"name": "foo"}})


def test_add(gateway: SyncApiGateway):
    response = gateway.add({"title": "test_add", "author": {"name": "foo"}})
    assert isinstance(response["id"], int)
    assert response["title"] == "test_add"
    assert response["author"] == {"name": "foo"}
    assert response["created_at"] == response["updated_at"]


def test_get(gateway: SyncApiGateway, book: Json):
    response = gateway.get(book["id"])
    assert response == book


def test_remove_and_404(gateway: SyncApiGateway, book: Json):
    assert gateway.remove(book["id"]) is True
    assert gateway.get(book["id"]) is None
    assert gateway.remove(book["id"]) is False


def test_update(gateway: SyncApiGateway, book: Json):
    response = gateway.update({"id": book["id"], "title": "test_update"})

    assert response["id"] == book["id"]
    assert response["title"] == "test_update"
    assert response["author"] == {"name": "foo"}
    assert response["created_at"] != response["updated_at"]


def test_update_404(gateway: SyncApiGateway):
    with pytest.raises(DoesNotExist):
        gateway.update({"id": 123456, "title": "test_update_404"})
