from http import HTTPStatus

import pytest

from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import SyncApiGateway
from clean_python.api_client import SyncApiProvider


class BooksGateway(SyncApiGateway, path="v1-alpha/books/{id}"):
    pass


@pytest.fixture
def provider(fastapi_example_app) -> SyncApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield SyncApiProvider(fastapi_example_app + "/", lambda a, b: "token")
    ctx.tenant = None


def test_request_params(provider: SyncApiProvider):
    response = provider.request(
        "GET", "v1-alpha/books", params={"limit": 10, "offset": 2}
    )

    assert isinstance(response, dict)

    assert response["limit"] == 10
    assert response["offset"] == 2


def test_request_json_body(provider: SyncApiProvider):
    response = provider.request(
        "POST", "v1-alpha/books", json={"title": "test_body", "author": {"name": "foo"}}
    )

    assert isinstance(response, dict)
    assert response["title"] == "test_body"
    assert response["author"] == {"name": "foo"}


@pytest.fixture
def book(provider: SyncApiProvider):
    return provider.request(
        "POST", "v1-alpha/books", json={"title": "fixture", "author": {"name": "foo"}}
    )


def test_no_content(provider: SyncApiProvider, book):
    response = provider.request("DELETE", f"v1-alpha/books/{book['id']}")

    assert response is None


def test_not_found(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1-alpha/book")

    assert e.value.status is HTTPStatus.NOT_FOUND
    assert e.value.args[0] == {"detail": "Not Found"}


def test_bad_request(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1-alpha/books", params={"limit": "foo"})

    assert e.value.status is HTTPStatus.BAD_REQUEST
    assert e.value.args[0]["detail"][0]["loc"] == ["query", "limit"]


def test_no_json_response(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1-alpha/text")

    assert e.value.args[0] == "Unexpected content type 'text/plain; charset=utf-8'"
