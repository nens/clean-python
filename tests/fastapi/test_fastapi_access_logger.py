from unittest import mock
from uuid import uuid4

import pytest
from fastapi.routing import APIRoute
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.responses import StreamingResponse

from clean_python import InMemoryGateway
from clean_python.fastapi import FastAPIAccessLogger
from clean_python.fastapi import get_correlation_id

SOME_UUID = uuid4()


@pytest.fixture
def fastapi_access_logger():
    return FastAPIAccessLogger(hostname="myhost", gateway=InMemoryGateway([]))


@pytest.fixture
def req():
    # a copy-paste from a local session, with some values removed / shortened
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "server": ("172.20.0.6", 80),
        "client": ("172.20.0.1", 45584),
        "scheme": "http",
        "root_path": "/v1-beta",
        "headers": [
            (b"host", b"localhost:8000"),
            (b"connection", b"keep-alive"),
            (b"accept", b"application/json"),
            (b"authorization", b"..."),
            (b"user-agent", b"Mozilla/5.0 ..."),
            (b"referer", b"http://localhost:8000/v1-beta/docs"),
            (b"accept-encoding", b"gzip, deflate, br"),
            (b"accept-language", b"en-US,en;q=0.9"),
            (b"cookie", b"..."),
            (b"x-correlation-id", str(SOME_UUID).encode()),
        ],
        "state": {},
        "method": "GET",
        "path": "/v1-beta/rasters",
        "raw_path": b"/v1-beta/rasters",
        "query_string": b"limit=50&offset=0&order_by=id",
        "path_params": {},
        "app_root_path": "",
        "route": APIRoute(
            endpoint=lambda x: x,
            path="/rasters",
            name="v1-beta/raster_list",
            methods=["GET"],
        ),
    }
    return Request(scope)


@pytest.fixture
def response():
    return JSONResponse({"foo": "bar"})


@pytest.fixture
def call_next(response):
    async def func(request):
        assert get_correlation_id(request) == SOME_UUID
        return response

    return func


@mock.patch("time.time", return_value=0.0)
async def test_logging(time, fastapi_access_logger, req, response, call_next):
    await fastapi_access_logger(req, call_next)
    assert len(fastapi_access_logger.gateway.data) == 0
    await response.background()
    (actual,) = fastapi_access_logger.gateway.data.values()
    actual.pop("id")
    assert actual == {
        "tag_suffix": "access_log",
        "remote_address": "172.20.0.1",
        "method": "GET",
        "path": "/v1-beta/rasters",
        "portal": "localhost:8000",
        "referer": "http://localhost:8000/v1-beta/docs",
        "user_agent": "Mozilla/5.0 ...",
        "query_params": "limit=50&offset=0&order_by=id",
        "view_name": "v1-beta/raster_list",
        "status": 200,
        "content_type": "application/json",
        "content_length": 13,
        "time": 0.0,
        "request_time": 0.0,
        "correlation_id": str(SOME_UUID),
    }


@pytest.fixture
def req_minimal():
    # https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "query_string": "",
        "headers": [(b"abc", b"def")],
    }
    return Request(scope)


@pytest.fixture
def streaming_response():
    async def numbers(minimum, maximum):
        yield ("<html><body><ul>")
        for number in range(minimum, maximum + 1):
            yield "<li>%d</li>" % number
        yield ("</ul></body></html>")

    return StreamingResponse(numbers(1, 3), media_type="text/html")


@pytest.fixture
def call_next_streaming(streaming_response):
    async def func(request):
        assert get_correlation_id(request) == SOME_UUID
        return streaming_response

    return func


@mock.patch("time.time", return_value=0.0)
@mock.patch("clean_python.fastapi.fastapi_access_logger.uuid4", return_value=SOME_UUID)
async def test_logging_minimal(
    time,
    uuid4,
    fastapi_access_logger,
    req_minimal,
    streaming_response,
    call_next_streaming,
):
    await fastapi_access_logger(req_minimal, call_next_streaming)
    assert req_minimal["headers"] == [
        (b"abc", b"def"),
        (b"x-correlation-id", str(SOME_UUID).encode()),
    ]
    assert len(fastapi_access_logger.gateway.data) == 0
    await streaming_response.background()
    (actual,) = fastapi_access_logger.gateway.data.values()
    actual.pop("id")
    assert actual == {
        "tag_suffix": "access_log",
        "remote_address": None,
        "method": "GET",
        "path": "/",
        "portal": "",
        "referer": None,
        "user_agent": None,
        "query_params": "",
        "view_name": None,
        "status": 200,
        "content_type": "text/html; charset=utf-8",
        "content_length": None,
        "time": 0.0,
        "request_time": 0.0,
        "correlation_id": str(SOME_UUID),
    }


@pytest.fixture
def req_health():
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "query_string": "",
        "headers": [],
        "route": APIRoute(
            endpoint=lambda x: x,
            path="/health",
            name="health_check",
            methods=["GET"],
        ),
    }
    return Request(scope)


@pytest.fixture
def call_next_no_correlation_id(response):
    async def func(request):
        assert get_correlation_id(request) is None
        return response

    return func


@mock.patch("time.time", return_value=0.0)
async def test_logging_health_check_skipped(
    time,
    fastapi_access_logger,
    req_health,
    streaming_response,
    call_next_no_correlation_id,
):
    await fastapi_access_logger(req_health, call_next_no_correlation_id)
    assert streaming_response.background is None
