from http import HTTPStatus

import pytest

from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import ApiProvider


@pytest.fixture
def provider(fastapi_example_app) -> ApiProvider:
    ctx.tenant = Tenant(id=2, name="")
    yield ApiProvider(fastapi_example_app + "/", lambda a, b: "token")
    ctx.tenant = None


async def test_request_params(provider: ApiProvider):
    response = await provider.request(
        "GET", "v1/books", params={"limit": 10, "offset": 2}
    )

    assert isinstance(response, dict)

    assert response["limit"] == 10
    assert response["offset"] == 2


async def test_request_json_body(provider: ApiProvider):
    response = await provider.request(
        "POST", "v1/books", json={"title": "test_body", "author": {"name": "foo"}}
    )

    assert isinstance(response, dict)
    assert response["title"] == "test_body"
    assert response["author"] == {"name": "foo"}


async def test_request_form_body(provider: ApiProvider):
    response = await provider.request("POST", "v1/form", fields={"name": "foo"})

    assert isinstance(response, dict)
    assert response["name"] == "foo"


# files are not supported (yet)
#
# async def test_request_form_file(provider: ApiProvider):
#     response = await provider.request("POST", "v1/file", fields={"file": ("x.txt", b"foo")})

#     assert isinstance(response, dict)
#     assert response["x.txt"] == "foo"


@pytest.fixture
async def book(provider: ApiProvider):
    return await provider.request(
        "POST", "v1/books", json={"title": "fixture", "author": {"name": "foo"}}
    )


async def test_no_content(provider: ApiProvider, book):
    response = await provider.request("DELETE", f"v1/books/{book['id']}")

    assert response is None


async def test_not_found(provider: ApiProvider):
    with pytest.raises(ApiException) as e:
        await provider.request("GET", "v1/book")

    assert e.value.status is HTTPStatus.NOT_FOUND
    assert e.value.args[0] == {"detail": "Not Found"}


async def test_bad_request(provider: ApiProvider):
    with pytest.raises(ApiException) as e:
        await provider.request("GET", "v1/books", params={"limit": "foo"})

    assert e.value.status is HTTPStatus.BAD_REQUEST
    assert e.value.args[0]["detail"][0]["loc"] == ["query", "limit"]


async def test_no_json_response(provider: ApiProvider):
    with pytest.raises(ApiException) as e:
        await provider.request("GET", "v1/text")

    assert e.value.args[0] == "Unexpected content type 'text/plain; charset=utf-8'"


async def test_urlencode(provider: ApiProvider):
    response = await provider.request("PUT", "v1/urlencode/x?")

    assert isinstance(response, dict)
    assert response["name"] == "x?"


async def test_request_raw(provider: ApiProvider, book):
    response = await provider.request_raw("GET", f"v1/books/{book['id']}")

    assert response.status is HTTPStatus.OK
    assert len(response.data) > 0
    assert response.content_type == "application/json"
