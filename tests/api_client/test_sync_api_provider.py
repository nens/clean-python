from http import HTTPStatus
from unittest import mock

import pytest

from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import SyncApiProvider

MODULE = "clean_python.api_client.api_provider"


@pytest.fixture
def tenant() -> Tenant:
    ctx.tenant = Tenant(id=2, name="")
    yield ctx.tenant
    ctx.tenant = None


@pytest.fixture
def response():
    response = mock.Mock()
    response.status = int(HTTPStatus.OK)
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def api_provider(tenant, response) -> SyncApiProvider:
    with mock.patch(MODULE + ".PoolManager"):
        api_provider = SyncApiProvider(
            url="http://testserver/foo/",
            fetch_token=lambda a, b: f"tenant-{b}",
        )
        api_provider._pool.request.return_value = response
        yield api_provider


def test_get(api_provider: SyncApiProvider, response):
    actual = api_provider.request("GET", "")

    assert api_provider._pool.request.call_count == 1
    assert api_provider._pool.request.call_args[1] == dict(
        method="GET",
        url="http://testserver/foo",
        json=None,
        fields=None,
        headers={"Authorization": "Bearer tenant-2"},
        timeout=5.0,
    )
    assert actual == response.json.return_value


def test_post_json(api_provider: SyncApiProvider, response):
    response.status == int(HTTPStatus.CREATED)
    api_provider._pool.request.return_value = response
    actual = api_provider.request("POST", "bar", json={"foo": 2})

    assert api_provider._pool.request.call_count == 1

    assert api_provider._pool.request.call_args[1] == dict(
        method="POST",
        url="http://testserver/foo/bar",
        json={"foo": 2},
        fields=None,
        headers={"Authorization": "Bearer tenant-2"},
        timeout=5.0,
    )
    assert actual == response.json.return_value


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
def test_url(api_provider: SyncApiProvider, path, params, expected_url):
    api_provider.request("GET", path, params=params)
    assert api_provider._pool.request.call_args[1]["url"] == expected_url


def test_timeout(api_provider: SyncApiProvider):
    api_provider.request("POST", "bar", timeout=2.1)
    assert api_provider._pool.request.call_args[1]["timeout"] == 2.1


@pytest.mark.parametrize(
    "status", [HTTPStatus.OK, HTTPStatus.NOT_FOUND, HTTPStatus.INTERNAL_SERVER_ERROR]
)
def test_unexpected_content_type(api_provider: SyncApiProvider, response, status):
    response.status = int(status)
    response.headers["Content-Type"] = "text/plain"
    with pytest.raises(ApiException) as e:
        api_provider.request("GET", "bar")

    assert e.value.status is status
    assert str(e.value) == "Unexpected content type 'text/plain'"


def test_no_content(api_provider: SyncApiProvider, response):
    response.status = int(HTTPStatus.NO_CONTENT)
    response.headers = {}

    actual = api_provider.request("DELETE", "bar/2")
    assert actual is not None


def test_404(api_provider: SyncApiProvider, response):
    response.status = int(HTTPStatus.NOT_FOUND)
    actual = api_provider.request("GET", "bar")
    assert actual is None


@pytest.mark.parametrize("status", [HTTPStatus.BAD_REQUEST, HTTPStatus.FORBIDDEN])
def test_error_response(api_provider: SyncApiProvider, response, status):
    response.status = int(status)

    with pytest.raises(ApiException) as e:
        api_provider.request("GET", "bar")

    assert e.value.status is status
    assert str(e.value) == str(response.json())


@mock.patch(MODULE + ".PoolManager", new=mock.Mock())
def test_no_token(response, tenant):
    api_provider = SyncApiProvider(
        url="http://testserver/foo/", fetch_token=lambda a, b: None
    )
    api_provider._pool.request.return_value = response
    api_provider.request("GET", "")
    assert api_provider._pool.request.call_args[1]["headers"] == {}
