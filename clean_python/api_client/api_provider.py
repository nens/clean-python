import asyncio
import re
from http import HTTPStatus
from typing import Callable
from typing import Optional
from urllib.parse import quote
from urllib.parse import urlencode
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientResponse
from aiohttp import ClientSession
from pydantic import AnyHttpUrl

from clean_python import ctx
from clean_python import Json

from .exceptions import ApiException
from .response import Response

__all__ = ["ApiProvider"]


RETRY_STATUSES = frozenset({413, 429, 503})  # like in urllib3


def is_success(status: HTTPStatus) -> bool:
    """Returns True on 2xx status"""
    return (int(status) // 100) == 2


JSON_CONTENT_TYPE_REGEX = re.compile(r"^application\/[^+]*[+]?(json);?.*$")


def is_json_content_type(content_type: Optional[str]) -> bool:
    if not content_type:
        return False
    return bool(JSON_CONTENT_TYPE_REGEX.match(content_type))


def join(url: str, path: str) -> str:
    """Results in a full url without trailing slash"""
    assert url.endswith("/")
    assert not path.startswith("/")
    result = urljoin(url, path)
    if result.endswith("/"):
        result = result[:-1]
    return result


def add_query_params(url: str, params: Optional[Json]) -> str:
    if params is None:
        return url
    return url + "?" + urlencode(params, doseq=True)


class ApiProvider:
    """Basic JSON API provider with retry policy and bearer tokens.

    The default retry policy has 3 retries with 1, 2, 4 second intervals.

    Args:
        url: The url of the API (with trailing slash)
        fetch_token: Callable that returns a token for a tenant id
        retries: Total number of retries per request
        backoff_factor: Multiplier for retry delay times (1, 2, 4, ...)
    """

    def __init__(
        self,
        url: AnyHttpUrl,
        fetch_token: Callable[[ClientSession, int], Optional[str]],
        retries: int = 3,
        backoff_factor: float = 1.0,
    ):
        self._url = str(url)
        assert self._url.endswith("/")
        self._fetch_token = fetch_token
        assert retries > 0
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._session = ClientSession()

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        params: Optional[Json],
        json: Optional[Json],
        fields: Optional[Json],
        timeout: float,
    ) -> ClientResponse:
        assert ctx.tenant is not None
        headers = {}
        request_kwargs = {
            "method": method,
            "url": add_query_params(join(self._url, quote(path)), params),
            "timeout": timeout,
            "json": json,
            "data": fields,
        }
        token = self._fetch_token(self._session, ctx.tenant.id)
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        for attempt in range(self._retries):
            if attempt > 0:
                backoff = self._backoff_factor * 2 ** (attempt - 1)
                await asyncio.sleep(backoff)

            try:
                response = await self._session.request(
                    headers=headers, **request_kwargs
                )
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
        timeout: float = 5.0,
    ) -> Optional[Json]:
        response = await self._request_with_retry(
            method, path, params, json, fields, timeout
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
        if is_success(status):
            return body
        else:
            raise ApiException(body, status=status)

    async def request_raw(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        timeout: float = 5.0,
    ) -> Response:
        response = await self._request_with_retry(
            method, path, params, json, fields, timeout
        )
        return Response(
            status=response.status,
            data=await response.read(),
            content_type=response.headers.get("Content-Type"),
        )
