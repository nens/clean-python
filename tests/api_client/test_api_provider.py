from asyncio.exceptions import TimeoutError
from http import HTTPStatus
from unittest import mock

import pytest
from aiohttp import ClientError
from aiohttp import ClientSession

from clean_python import Conflict
from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import ApiProvider

MODULE = "clean_python.api_client.api_provider"


async def fake_token():
    return {"Authorization": f"Bearer tenant-{ctx.tenant.id}"}


async def no_token():
    return {}


@pytest.fixture
def tenant() -> Tenant:
    ctx.tenant = Tenant(id=2, name="")
    yield ctx.tenant
    ctx.tenant = None


@pytest.fixture
def response():
    # this mocks the aiohttp.ClientResponse:
    response = mock.Mock()
    response.status = int(HTTPStatus.OK)
    response.headers = {"Content-Type": "application/json"}
    response.json = mock.AsyncMock(return_value={"foo": 2})
    response.read = mock.AsyncMock()
    return response


@pytest.fixture
async def api_provider_no_mock() -> mock.AsyncMock:
    provider = ApiProvider(
        url="http://testserver/foo/",
        headers_factory=fake_token,
        retries=0,
    )
    await provider.connect()
    yield provider
    await provider.disconnect()


@pytest.fixture
def request_m() -> mock.AsyncMock:
    request = mock.AsyncMock()
    with mock.patch.object(ClientSession, "request", new=request):
        yield request


@pytest.fixture
def api_provider(api_provider_no_mock, tenant, response, request_m) -> ApiProvider:
    request_m.return_value = response
    return api_provider_no_mock


async def test_get(api_provider: ApiProvider, request_m):
    actual = await api_provider.request("GET", "")

    assert request_m.call_count == 1
    assert request_m.call_args[1] == dict(
        method="GET",
        url="http://testserver/foo",
        headers={"Authorization": "Bearer tenant-2"},
        timeout=5.0,
        data=None,
        json=None,
    )
    assert actual == {"foo": 2}


async def test_post_json(api_provider: ApiProvider, response, request_m):
    response.status == int(HTTPStatus.CREATED)
    request_m.return_value = response
    actual = await api_provider.request("POST", "bar", json={"foo": 2})

    assert request_m.call_count == 1

    assert request_m.call_args[1] == dict(
        method="POST",
        url="http://testserver/foo/bar",
        data=None,
        json={"foo": 2},
        headers={
            "Authorization": "Bearer tenant-2",
        },
        timeout=5.0,
    )
    assert actual == {"foo": 2}


@pytest.mark.parametrize(
    "path,params,expected_url",
    [
        ("", None, "http://testserver/foo"),
        ("bar", None, "http://testserver/foo/bar"),
        ("bar/", None, "http://testserver/foo/bar"),
        ("", {"a": 2}, "http://testserver/foo?a=2"),
        ("bar", {"a": 2}, "http://testserver/foo/bar?a=2"),
        ("bar/", {"a": 2}, "http://testserver/foo/bar?a=2"),
        ("", {"a": [1, 2]}, "http://testserver/foo?a=1&a=2"),
        ("", {"a": 1, "b": "foo"}, "http://testserver/foo?a=1&b=foo"),
    ],
)
async def test_url(api_provider: ApiProvider, path, params, expected_url, request_m):
    await api_provider.request("GET", path, params=params)
    assert request_m.call_args[1]["url"] == expected_url


async def test_timeout(api_provider: ApiProvider, request_m):
    await api_provider.request("POST", "bar", timeout=2.1)
    assert request_m.call_args[1]["timeout"] == 2.1


@pytest.mark.parametrize(
    "status", [HTTPStatus.OK, HTTPStatus.NOT_FOUND, HTTPStatus.INTERNAL_SERVER_ERROR]
)
async def test_unexpected_content_type(api_provider: ApiProvider, response, status):
    response.status = int(status)
    response.headers["Content-Type"] = "text/plain"
    with pytest.raises(ApiException) as e:
        await api_provider.request("GET", "bar")

    assert e.value.status is status
    assert str(e.value) == f"{status}: Unexpected content type 'text/plain'"


async def test_json_variant_content_type(api_provider: ApiProvider, response):
    response.headers["Content-Type"] = "application/something+json"
    actual = await api_provider.request("GET", "bar")
    assert actual == {"foo": 2}


async def test_no_content(api_provider: ApiProvider, response):
    response.status = int(HTTPStatus.NO_CONTENT)
    response.headers = {}

    actual = await api_provider.request("DELETE", "bar/2")
    assert actual is None


@pytest.mark.parametrize("status", [HTTPStatus.BAD_REQUEST, HTTPStatus.NOT_FOUND])
async def test_error_response(api_provider: ApiProvider, response, status):
    response.status = int(status)

    with pytest.raises(ApiException) as e:
        await api_provider.request("GET", "bar")

    assert e.value.status is status
    assert str(e.value) == str(int(status)) + ": {'foo': 2}"


async def test_no_token(api_provider: ApiProvider, request_m):
    api_provider._headers_factory = no_token
    await api_provider.request("GET", "")
    assert request_m.call_args[1]["headers"] == {}


@pytest.mark.parametrize(
    "path,trailing_slash,expected",
    [
        ("bar", False, "bar"),
        ("bar", True, "bar/"),
        ("bar/", False, "bar"),
        ("bar/", True, "bar/"),
    ],
)
async def test_trailing_slash(
    api_provider: ApiProvider, path, trailing_slash, expected, request_m
):
    api_provider._trailing_slash = trailing_slash
    await api_provider.request("GET", path)

    assert request_m.call_args[1]["url"] == "http://testserver/foo/" + expected


async def test_conflict(api_provider: ApiProvider, response):
    response.status = HTTPStatus.CONFLICT

    with pytest.raises(Conflict):
        await api_provider.request("GET", "bar")


async def test_conflict_with_message(api_provider: ApiProvider, response):
    response.status = HTTPStatus.CONFLICT
    response.json.return_value = {"message": "foo"}

    with pytest.raises(Conflict, match="foo"):
        await api_provider.request("GET", "bar")


async def test_custom_header(api_provider: ApiProvider, request_m):
    await api_provider.request("POST", "bar", headers={"foo": "bar"})
    assert request_m.call_args[1]["headers"] == {
        "foo": "bar",
        **(await api_provider._headers_factory()),
    }


async def test_custom_header_precedes(api_provider: ApiProvider, request_m):
    await api_provider.request("POST", "bar", headers={"Authorization": "bar"})
    assert request_m.call_args[1]["headers"]["Authorization"] == "bar"


async def test_disconnect(api_provider: ApiProvider, request_m):
    with mock.patch.object(
        api_provider._session, "close", new_callable=mock.AsyncMock
    ) as close_m:
        await api_provider.disconnect()

    close_m.assert_awaited_once()


@pytest.fixture
async def retry_provider():
    provider = ApiProvider(
        url="http://testserver/foo/", retries=1, backoff_factor=0.001
    )
    await provider.connect()
    yield provider
    await provider.disconnect()


@pytest.fixture
def error_response():
    # this mocks the aiohttp.ClientResponse:
    response = mock.Mock()
    response.status = int(HTTPStatus.SERVICE_UNAVAILABLE)
    response.headers = {"Content-Type": "text/html"}
    response.read = mock.AsyncMock()
    return response


@pytest.mark.parametrize("error_cls", [ClientError, TimeoutError])
@mock.patch.object(ClientSession, "request", new_callable=mock.AsyncMock)
async def test_retry_client_error(
    request_m, retry_provider: ApiProvider, error_cls, response
):
    request_m.side_effect = (error_cls(), response)

    actual = await retry_provider.request("GET", "")

    assert request_m.call_count == 2
    assert actual == {"foo": 2}


@mock.patch.object(ClientSession, "request", new_callable=mock.AsyncMock)
async def test_retry_client_error_too_many(request_m, retry_provider: ApiProvider):
    request_m.side_effect = (ClientError("bar"), ClientError("foo"))

    with pytest.raises(ClientError, match="foo"):
        await retry_provider.request("GET", "")

    assert request_m.call_count == 2


@pytest.mark.parametrize("error_code", [429, 500, 502, 503, 504])
@mock.patch.object(ClientSession, "request", new_callable=mock.AsyncMock)
async def test_retry_error_response(
    request_m, retry_provider: ApiProvider, error_code: int, response, error_response
):
    error_response.status = error_code
    request_m.side_effect = (error_response, response)

    actual = await retry_provider.request("GET", "")

    assert request_m.call_count == 2
    assert actual == {"foo": 2}


@mock.patch.object(ClientSession, "request", new_callable=mock.AsyncMock)
async def test_retry_error_response_too_many(
    request_m, retry_provider: ApiProvider, error_response
):
    request_m.return_value = error_response

    with pytest.raises(ApiException) as e:
        await retry_provider.request("GET", "")

    assert request_m.call_count == 2
    assert e.value.status == 503


@mock.patch.object(ClientSession, "request", new_callable=mock.AsyncMock)
async def test_no_retry_on_post(request_m, retry_provider: ApiProvider):
    request_m.side_effect = ClientError()

    with pytest.raises(ClientError):
        await retry_provider.request("POST", "")

    assert request_m.call_count == 1
