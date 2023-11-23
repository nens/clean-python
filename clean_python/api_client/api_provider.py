import asyncio
import re
from http import HTTPStatus
from io import BytesIO
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Optional
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


RETRY_STATUSES = frozenset({413, 429, 503})  # like in urllib3


def is_success(status: HTTPStatus) -> bool:
    """Returns True on 2xx status"""
    return (int(status) // 100) == 2


def check_exception(status: HTTPStatus, body: Json) -> None:
    if status == HTTPStatus.CONFLICT:
        raise Conflict(body.get("message", str(body)))
    elif not is_success(status):
        raise ApiException(body, status=status)


JSON_CONTENT_TYPE_REGEX = re.compile(r"^application\/[^+]*[+]?(json);?.*$")


def is_json_content_type(content_type: Optional[str]) -> bool:
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


def add_query_params(url: str, params: Optional[Json]) -> str:
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
        headers_factory: Optional[Callable[[], Awaitable[Dict[str, str]]]] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        trailing_slash: bool = False,
    ):
        self._url = str(url)
        if not self._url.endswith("/"):
            self._url += "/"
        self._headers_factory = headers_factory
        assert retries > 0
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._trailing_slash = trailing_slash
        self._session = ClientSession()

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        params: Optional[Json],
        json: Optional[Json],
        fields: Optional[Json],
        file: Optional[FileFormPost],
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
        if self._headers_factory is not None:
            request_kwargs["headers"] = await self._headers_factory()
        for attempt in range(self._retries):
            if attempt > 0:
                backoff = self._backoff_factor * 2 ** (attempt - 1)
                await asyncio.sleep(backoff)

            try:
                response = await self._session.request(**request_kwargs)
                await response.read()
            except (aiohttp.ClientError, asyncio.exceptions.TimeoutError):
                if attempt == self._retries - 1:
                    raise  # propagate ClientError in case no retries left
            else:
                if response.status not in RETRY_STATUSES:
                    return response  # on all non-retry statuses: return response

        return response  # retries exceeded; return the (possibly error) response

    async def request(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        file: Optional[FileFormPost] = None,
        timeout: float = 5.0,
    ) -> Optional[Json]:
        response = await self._request_with_retry(
            method, path, params, json, fields, file, timeout
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
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        file: Optional[FileFormPost] = None,
        timeout: float = 5.0,
    ) -> Response:
        response = await self._request_with_retry(
            method, path, params, json, fields, file, timeout
        )
        return Response(
            status=response.status,
            data=await response.read(),
            content_type=response.headers.get("Content-Type"),
        )
