import json as json_lib
from http import HTTPStatus
from typing import Callable
from typing import Dict
from typing import Optional
from urllib.parse import quote

from pydantic import AnyHttpUrl
from urllib3 import PoolManager
from urllib3 import Retry

from clean_python import Json

from .api_provider import add_query_params
from .api_provider import check_exception
from .api_provider import FileFormPost
from .api_provider import is_json_content_type
from .api_provider import join
from .exceptions import ApiException
from .response import Response

__all__ = ["SyncApiProvider"]


class SyncApiProvider:
    """Basic JSON API provider with retry policy and bearer tokens.

    The default retry policy has 3 retries with 1, 2, 4 second intervals.

    Args:
        url: The url of the API (with trailing slash)
        headers_factory: Callable that returns headers (for e.g. authorization)
        retries: Total number of retries per request
        backoff_factor: Multiplier for retry delay times (1, 2, 4, ...)
        trailing_slash: Wether to automatically add or remove trailing slashes.
    """

    def __init__(
        self,
        url: AnyHttpUrl,
        headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
        retries: int = 3,
        backoff_factor: float = 1.0,
        trailing_slash: bool = False,
    ):
        self._url = str(url)
        if not self._url.endswith("/"):
            self._url += "/"
        self._headers_factory = headers_factory
        self._pool = PoolManager(retries=Retry(retries, backoff_factor=backoff_factor))
        self._trailing_slash = trailing_slash

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Json],
        json: Optional[Json],
        fields: Optional[Json],
        file: Optional[FileFormPost],
        timeout: float,
    ):
        headers = {}
        request_kwargs = {
            "method": method,
            "url": add_query_params(
                join(self._url, quote(path), self._trailing_slash), params
            ),
            "timeout": timeout,
        }
        # for urllib3<2, we dump json ourselves
        if json is not None and fields is not None:
            raise ValueError("Cannot both specify 'json' and 'fields'")
        elif json is not None and file is not None:
            raise ValueError("Cannot both specify 'json' and 'file'")
        elif json is not None:
            request_kwargs["body"] = json_lib.dumps(json).encode()
            headers["Content-Type"] = "application/json"
        elif fields is not None and file is None:
            request_kwargs["fields"] = fields
            request_kwargs["encode_multipart"] = False
        elif file is not None:
            request_kwargs["fields"] = {
                file.field_name: (
                    file.file_name,
                    file.file.read(),
                    file.content_type,
                ),
                **(fields or {}),
            }
            request_kwargs["encode_multipart"] = True

        if self._headers_factory is not None:
            headers.update(self._headers_factory())
        return self._pool.request(headers=headers, **request_kwargs)

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        file: Optional[FileFormPost] = None,
        timeout: float = 5.0,
    ) -> Optional[Json]:
        response = self._request(method, path, params, json, fields, file, timeout)
        status = HTTPStatus(response.status)
        content_type = response.headers.get("Content-Type")
        if status is HTTPStatus.NO_CONTENT:
            return None
        if not is_json_content_type(content_type):
            raise ApiException(
                f"Unexpected content type '{content_type}'", status=status
            )
        body = json_lib.loads(response.data.decode())
        check_exception(status, body)
        return body

    def request_raw(
        self,
        method: str,
        path: str,
        params: Optional[Json] = None,
        json: Optional[Json] = None,
        fields: Optional[Json] = None,
        file: Optional[FileFormPost] = None,
        timeout: float = 5.0,
    ) -> Response:
        response = self._request(method, path, params, json, fields, file, timeout)
        return Response(
            status=response.status,
            data=response.data,
            content_type=response.headers.get("Content-Type"),
        )
