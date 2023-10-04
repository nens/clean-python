# This module is a copy paste of test_int_api_provider.py

from http import HTTPStatus

import pytest

from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import SyncApiProvider


@pytest.fixture
def provider(fastapi_example_app) -> SyncApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield SyncApiProvider(fastapi_example_app + "/", lambda a, b: "token")
    ctx.tenant = None


def test_request_params(provider: SyncApiProvider):
    response = provider.request("GET", "v1/books", params={"limit": 10, "offset": 2})

    assert isinstance(response, dict)

    assert response["limit"] == 10
    assert response["offset"] == 2


def test_request_json_body(provider: SyncApiProvider):
    response = provider.request(
        "POST", "v1/books", json={"title": "test_body", "author": {"name": "foo"}}
    )

    assert isinstance(response, dict)
    assert response["title"] == "test_body"
    assert response["author"] == {"name": "foo"}


def test_request_form_body(provider: SyncApiProvider):
    response = provider.request("POST", "v1/form", fields={"name": "foo"})

    assert isinstance(response, dict)
    assert response["name"] == "foo"


def test_request_form_file(provider: SyncApiProvider):
    response = provider.request("POST", "v1/file", fields={"file": ("x.txt", b"foo")})

    assert isinstance(response, dict)
    assert response["x.txt"] == "foo"


@pytest.fixture
def book(provider: SyncApiProvider):
    return provider.request(
        "POST", "v1/books", json={"title": "fixture", "author": {"name": "foo"}}
    )


def test_no_content(provider: SyncApiProvider, book):
    response = provider.request("DELETE", f"v1/books/{book['id']}")

    assert response is None


def test_not_found(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1/book")

    assert e.value.status is HTTPStatus.NOT_FOUND
    assert e.value.args[0] == {"detail": "Not Found"}


def test_bad_request(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1/books", params={"limit": "foo"})

    assert e.value.status is HTTPStatus.BAD_REQUEST
    assert e.value.args[0]["detail"][0]["loc"] == ["query", "limit"]


def test_no_json_response(provider: SyncApiProvider):
    with pytest.raises(ApiException) as e:
        provider.request("GET", "v1/text")

    assert e.value.args[0] == "Unexpected content type 'text/plain; charset=utf-8'"


def test_urlencode(provider: SyncApiProvider):
    response = provider.request("PUT", "v1/urlencode/x?")

    assert isinstance(response, dict)
    assert response["name"] == "x?"


def test_request_raw(provider: SyncApiProvider, book):
    response = provider.request_raw("GET", f"v1/books/{book['id']}")

    assert response.status is HTTPStatus.OK
    assert len(response.data) > 0
    assert response.content_type == "application/json"
