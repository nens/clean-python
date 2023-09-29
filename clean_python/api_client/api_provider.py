import re
from http import HTTPStatus
from typing import Callable
from typing import Optional
from urllib.parse import urlencode
from urllib.parse import urljoin

from pydantic import AnyHttpUrl
from urllib3 import PoolManager
from urllib3 import Retry

from clean_python import ctx
from clean_python import Json

from .exceptions import ApiException

__all__ = ["SyncApiProvider"]


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

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        timeout: float = 5.0,
    ) -> Optional[Json]:
        assert ctx.tenant is not None
        url = join(self._url, path)
        token = self._fetch_token(self._pool, ctx.tenant.id)
        headers = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        response = self._pool.request(
            method=method,
            url=add_query_params(url, params),
            json=json,
            fields=fields,
            headers=headers,
            timeout=timeout,
        )
        status = HTTPStatus(response.status)
        content_type = response.headers.get("Content-Type")
        if status is HTTPStatus.NO_CONTENT:
            return None
        if not is_json_content_type(content_type):
            raise ApiException(
                f"Unexpected content type '{content_type}'", status=status
            )
        body = response.json()
        if is_success(status):
            return body
        else:
            raise ApiException(body, status=status)
