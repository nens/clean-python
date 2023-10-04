import json as json_lib
from http import HTTPStatus
from typing import Callable
from typing import Optional
from urllib.parse import quote

from pydantic import AnyHttpUrl
from urllib3 import PoolManager
from urllib3 import Retry

from clean_python import ctx
from clean_python import Json

from .api_provider import add_query_params
from .api_provider import is_json_content_type
from .api_provider import is_success
from .api_provider import join
from .exceptions import ApiException
from .response import Response

__all__ = ["SyncApiProvider"]


class SyncApiProvider:
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
        fetch_token: Callable[[PoolManager, int], Optional[str]],
        retries: int = 3,
        backoff_factor: float = 1.0,
    ):
        self._url = str(url)
        assert self._url.endswith("/")
        self._fetch_token = fetch_token
        self._pool = PoolManager(retries=Retry(retries, backoff_factor=backoff_factor))

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Json],
        json: Optional[Json],
        fields: Optional[Json],
        timeout: float,
    ):
        assert ctx.tenant is not None
        headers = {}
        request_kwargs = {
            "method": method,
            "url": add_query_params(join(self._url, quote(path)), params),
            "timeout": timeout,
        }
        # for urllib3<2, we dump json ourselves
        if json is not None and fields is not None:
            raise ValueError("Cannot both specify 'json' and 'fields'")
        elif json is not None:
            request_kwargs["body"] = json_lib.dumps(json).encode()
            headers["Content-Type"] = "application/json"
        elif fields is not None:
            request_kwargs["fields"] = fields
        token = self._fetch_token(self._pool, ctx.tenant.id)
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        return self._pool.request(headers=headers, **request_kwargs)

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        timeout: float = 5.0,
    ) -> Optional[Json]:
        response = self._request(method, path, params, json, fields, timeout)
        status = HTTPStatus(response.status)
        content_type = response.headers.get("Content-Type")
        if status is HTTPStatus.NO_CONTENT:
            return None
        if not is_json_content_type(content_type):
            raise ApiException(
                f"Unexpected content type '{content_type}'", status=status
            )
        body = json_lib.loads(response.data.decode())
        if is_success(status):
            return body
        else:
            raise ApiException(body, status=status)

    def request_raw(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        timeout: float = 5.0,
    ) -> Response:
        response = self._request(method, path, params, json, fields, timeout)
        return Response(
            status=response.status,
            data=response.data,
            content_type=response.headers.get("Content-Type"),
        )
