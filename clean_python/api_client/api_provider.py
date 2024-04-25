import asyncio
import re
from collections.abc import Awaitable
from collections.abc import Callable
from http import HTTPStatus
from io import BytesIO
from typing import Any
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientResponse
from aiohttp import ClientSession
from pydantic import AnyHttpUrl
from pydantic import field_validator

from clean_python import Conflict
from clean_python import Json
from clean_python import ValueObject

from .exceptions import ApiException
from .response import Response

__all__ = ["ApiProvider", "FileFormPost"]


# Retry on 429 and all 5xx errors (because they are mostly temporary)
RETRY_STATUSES = frozenset(
    {
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }
)
# PATCH is strictly not idempotent, because you could do advanced
# JSON operations like 'add an array element'. mostly idempotent.
# However we never do that and we always make PATCH idempotent.
RETRY_METHODS = frozenset(["HEAD", "GET", "PATCH", "PUT", "DELETE", "OPTIONS", "TRACE"])


def is_success(status: HTTPStatus) -> bool:
    """Returns True on 2xx status"""
    return (int(status) // 100) == 2


def check_exception(status: HTTPStatus, body: Json) -> None:
    if status == HTTPStatus.CONFLICT:
        raise Conflict(body.get("message", str(body)))
    elif not is_success(status):
        raise ApiException(body, status=status)


JSON_CONTENT_TYPE_REGEX = re.compile(r"^application\/[^+]*[+]?(json);?.*$")


def is_json_content_type(content_type: str | None) -> bool:
    if not content_type:
        return False
    return bool(JSON_CONTENT_TYPE_REGEX.match(content_type))


def join(url: str, path: str, trailing_slash: bool = False) -> str:
    """Results in a full url without trailing slash"""
    assert url.endswith("/")
    assert not path.startswith("/")
    result = urljoin(url, path)
    if trailing_slash and not result.endswith("/"):
        result = result + "/"
    elif not trailing_slash and result.endswith("/"):
        result = result[:-1]
    return result


def add_query_params(url: str, params: Json | None) -> str:
    if params is None:
        return url
    return url + "?" + urlencode(params, doseq=True)


class FileFormPost(ValueObject):
    file_name: str
    file: Any  # typing of BinaryIO / BytesIO is hard!
    field_name: str = "file"
    content_type: str = "application/octet-stream"

    @field_validator("file")
    @classmethod
    def validate_file(cls, v):
        if isinstance(v, bytes):
            return BytesIO(v)
        assert hasattr(v, "read")  # poor-mans BinaryIO validation
        return v


class ApiProvider:
    """Basic JSON API provider with retry policy and bearer tokens.

    The default retry policy has 3 retries with 1, 2, 4 second intervals.

    Args:
        url: The url of the API (with trailing slash)
        headers_factory: Coroutine that returns headers (for e.g. authorization)
        retries: Total number of retries per request
        backoff_factor: Multiplier for retry delay times (1, 2, 4, ...)
        trailing_slash: Wether to automatically add or remove trailing slashes.
    """

    def __init__(
        self,
        url: AnyHttpUrl,
        headers_factory: Callable[[], Awaitable[dict[str, str]]] | None = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        trailing_slash: bool = False,
    ):
        self._url = str(url)
        if not self._url.endswith("/"):
            self._url += "/"
        self._headers_factory = headers_factory
        assert retries >= 0
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._trailing_slash = trailing_slash

    @property
    def _session(self) -> ClientSession:
        # There seems to be an issue if the ClientSession is instantiated before
        # the event loop runs. So we do that delayed in a property. Use this property
        # in a context manager.
        # TODO It is more efficient to reuse the connection / connection pools. One idea
        # is to expose .session as a context manager (like with the SQLProvider.transaction)
        return ClientSession()

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        params: Json | None,
        json: Json | None,
        fields: Json | None,
        file: FileFormPost | None,
        headers: dict[str, str] | None,
        timeout: float,
    ) -> ClientResponse:
        if file is not None:
            raise NotImplementedError("ApiProvider doesn't yet support file uploads")
        request_kwargs = {
            "method": method,
            "url": add_query_params(
                join(self._url, quote(path), self._trailing_slash), params
            ),
            "timeout": timeout,
            "json": json,
            "data": fields,
        }
        actual_headers = {}
        if self._headers_factory is not None:
            actual_headers.update(await self._headers_factory())
        if headers:
            actual_headers.update(headers)
        retries = self._retries if method.upper() in RETRY_METHODS else 0
        for attempt in range(retries + 1):
            if attempt > 0:
                backoff = self._backoff_factor * 2 ** (attempt - 1)
                await asyncio.sleep(backoff)

            try:
                async with self._session as session:
                    response = await session.request(
                        headers=actual_headers, **request_kwargs
                    )
                    if response.status in RETRY_STATUSES:
                        continue
                    await response.read()
                    return response
            except (aiohttp.ClientError, asyncio.exceptions.TimeoutError):
                if attempt == retries:
                    raise  # propagate ClientError in case no retries left

        return response  # retries exceeded; return the (possibly error) response

    async def request(
        self,
        method: str,
        path: str,
        params: Json | None = None,
        json: Json | None = None,
        fields: Json | None = None,
        file: FileFormPost | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 5.0,
    ) -> Json | None:
        response = await self._request_with_retry(
            method, path, params, json, fields, file, headers, timeout
        )
        status = HTTPStatus(response.status)
        content_type = response.headers.get("Content-Type")
        if status is HTTPStatus.NO_CONTENT:
            return None
        if not is_json_content_type(content_type):
            raise ApiException(
                f"Unexpected content type '{content_type}'", status=status
            )
        body = await response.json()
        check_exception(status, body)
        return body

    async def request_raw(
        self,
        method: str,
        path: str,
        params: Json | None = None,
        json: Json | None = None,
        fields: Json | None = None,
        file: FileFormPost | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 5.0,
    ) -> Response:
        response = await self._request_with_retry(
            method, path, params, json, fields, file, headers, timeout
        )
        return Response(
            status=response.status,
            data=await response.read(),
            content_type=response.headers.get("Content-Type"),
        )
