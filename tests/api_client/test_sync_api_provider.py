# This module is a copy paste of test_api_provider.py

from http import HTTPStatus
from unittest import mock

import pytest

from clean_python import Conflict
from clean_python import ctx
from clean_python import Tenant
from clean_python.api_client import ApiException
from clean_python.api_client import FileFormPost
from clean_python.api_client import SyncApiProvider

MODULE = "clean_python.api_client.sync_api_provider"


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
    response.data = b'{"foo": 2}'
    return response


@pytest.fixture
def api_provider(tenant, response) -> SyncApiProvider:
    with mock.patch(MODULE + ".PoolManager"):
        api_provider = SyncApiProvider(
            url="http://testserver/foo/",
            headers_factory=lambda: {"Authorization": f"Bearer tenant-{ctx.tenant.id}"},
        )
        api_provider._pool.request.return_value = response
        yield api_provider


def test_get(api_provider: SyncApiProvider, response):
    actual = api_provider.request("GET", "")

    assert api_provider._pool.request.call_count == 1
    assert api_provider._pool.request.call_args[1] == dict(
        method="GET",
        url="http://testserver/foo",
        headers={"Authorization": "Bearer tenant-2"},
        timeout=5.0,
    )
    assert actual == {"foo": 2}


def test_post_json(api_provider: SyncApiProvider, response):
    response.status == int(HTTPStatus.CREATED)
    api_provider._pool.request.return_value = response
    actual = api_provider.request("POST", "bar", json={"foo": 2})

    assert api_provider._pool.request.call_count == 1

    assert api_provider._pool.request.call_args[1] == dict(
        method="POST",
        url="http://testserver/foo/bar",
        body=b'{"foo": 2}',
        headers={
            "Content-Type": "application/json",
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
        ("", {"a": None}, "http://testserver/foo"),
        ("", {"a": ""}, "http://testserver/foo?a="),
        ("", {"a": []}, "http://testserver/foo"),
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
    assert str(e.value) == f"{status}: Unexpected content type 'text/plain'"


def test_json_variant_content_type(api_provider: SyncApiProvider, response):
    response.headers["Content-Type"] = "application/something+json"
    actual = api_provider.request("GET", "bar")
    assert actual == {"foo": 2}


def test_no_content(api_provider: SyncApiProvider, response):
    response.status = int(HTTPStatus.NO_CONTENT)
    response.headers = {}

    actual = api_provider.request("DELETE", "bar/2")
    assert actual is None


@pytest.mark.parametrize("status", [HTTPStatus.BAD_REQUEST, HTTPStatus.NOT_FOUND])
def test_error_response(api_provider: SyncApiProvider, response, status):
    response.status = int(status)

    with pytest.raises(ApiException) as e:
        api_provider.request("GET", "bar")

    assert e.value.status is status
    assert str(e.value) == str(int(status)) + ": {'foo': 2}"


@mock.patch(MODULE + ".PoolManager", new=mock.Mock())
def test_no_token(response, tenant):
    api_provider = SyncApiProvider(
        url="http://testserver/foo/", headers_factory=lambda: {}
    )
    api_provider._pool.request.return_value = response
    api_provider.request("GET", "")
    assert api_provider._pool.request.call_args[1]["headers"] == {}


@pytest.mark.parametrize(
    "path,trailing_slash,expected",
    [
        ("bar", False, "bar"),
        ("bar", True, "bar/"),
        ("bar/", False, "bar"),
        ("bar/", True, "bar/"),
    ],
)
def test_trailing_slash(api_provider: SyncApiProvider, path, trailing_slash, expected):
    api_provider._trailing_slash = trailing_slash
    api_provider.request("GET", path)

    assert (
        api_provider._pool.request.call_args[1]["url"]
        == "http://testserver/foo/" + expected
    )


def test_post_file(api_provider: SyncApiProvider):
    api_provider.request(
        "POST",
        "bar",
        file=FileFormPost(file_name="test.zip", file=b"foo", field_name="x"),
    )

    assert api_provider._pool.request.call_count == 1

    assert api_provider._pool.request.call_args[1] == dict(
        method="POST",
        url="http://testserver/foo/bar",
        fields={"x": ("test.zip", b"foo", "application/octet-stream")},
        headers={
            "Authorization": "Bearer tenant-2",
        },
        timeout=5.0,
        encode_multipart=True,
    )


def test_post_file_with_fields(api_provider: SyncApiProvider):
    api_provider.request(
        "POST",
        "bar",
        fields={"a": "b"},
        file=FileFormPost(file_name="test.zip", file=b"foo", field_name="x"),
    )

    assert api_provider._pool.request.call_count == 1

    assert api_provider._pool.request.call_args[1] == dict(
        method="POST",
        url="http://testserver/foo/bar",
        fields={"a": "b", "x": ("test.zip", b"foo", "application/octet-stream")},
        headers={
            "Authorization": "Bearer tenant-2",
        },
        timeout=5.0,
        encode_multipart=True,
    )


def test_conflict(api_provider: SyncApiProvider, response):
    response.status = HTTPStatus.CONFLICT

    with pytest.raises(Conflict):
        api_provider.request("GET", "bar")


def test_conflict_with_message(api_provider: SyncApiProvider, response):
    response.status = HTTPStatus.CONFLICT
    response.json.return_value = {"message": "foo"}

    with pytest.raises(Conflict, match="foo"):
        api_provider.request("GET", "bar")


def test_custom_header(api_provider: SyncApiProvider):
    api_provider.request("POST", "bar", headers={"foo": "bar"})
    assert api_provider._pool.request.call_args[1]["headers"] == {
        "foo": "bar",
        **api_provider._headers_factory(),
    }


def test_custom_header_precedes(api_provider: SyncApiProvider):
    api_provider.request("POST", "bar", headers={"Authorization": "bar"})
    assert api_provider._pool.request.call_args[1]["headers"]["Authorization"] == "bar"
