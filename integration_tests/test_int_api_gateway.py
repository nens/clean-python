import pytest

from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Json
from clean_python import Tenant
from clean_python.api_client import ApiGateway
from clean_python.api_client import ApiProvider


class BooksGateway(ApiGateway, path="v1/books/{id}"):
    pass


async def fake_token(a, b):
    return "token"


@pytest.fixture
def provider(fastapi_example_app) -> ApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield ApiProvider(fastapi_example_app + "/", fake_token)
    ctx.tenant = None


@pytest.fixture
def gateway(provider) -> ApiGateway:
    return BooksGateway(provider)


@pytest.fixture
async def book(gateway: ApiGateway):
    return await gateway.add({"title": "fixture", "author": {"name": "foo"}})


async def test_add(gateway: ApiGateway):
    response = await gateway.add({"title": "test_add", "author": {"name": "foo"}})
    assert isinstance(response["id"], int)
    assert response["title"] == "test_add"
    assert response["author"] == {"name": "foo"}
    assert response["created_at"] == response["updated_at"]


async def test_get(gateway: ApiGateway, book: Json):
    response = await gateway.get(book["id"])
    assert response == book


async def test_remove_and_404(gateway: ApiGateway, book: Json):
    assert await gateway.remove(book["id"]) is True
    assert await gateway.get(book["id"]) is None
    assert await gateway.remove(book["id"]) is False


async def test_update(gateway: ApiGateway, book: Json):
    response = await gateway.update({"id": book["id"], "title": "test_update"})

    assert response["id"] == book["id"]
    assert response["title"] == "test_update"
    assert response["author"] == {"name": "foo"}
    assert response["created_at"] != response["updated_at"]


async def test_update_404(gateway: ApiGateway):
    with pytest.raises(DoesNotExist):
        await gateway.update({"id": 123456, "title": "test_update_404"})
