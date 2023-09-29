import pytest

from clean_python import ctx
from clean_python import Json
from clean_python import Tenant
from clean_python.api_client import SyncApiGateway
from clean_python.api_client import SyncApiProvider


class BooksGateway(SyncApiGateway, path="v1-alpha/books/{id}"):
    pass


@pytest.fixture
def provider(fastapi_example_app) -> SyncApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield SyncApiProvider(fastapi_example_app + "/", lambda a, b: "token")
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
