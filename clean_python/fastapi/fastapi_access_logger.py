# (c) Nelen & Schuurmans

import os
import time
from typing import Awaitable
from typing import Callable
from typing import Optional
from uuid import UUID
from uuid import uuid4

import inject
from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import Response

from clean_python import Gateway
from clean_python.fluentbit import FluentbitGateway

__all__ = ["FastAPIAccessLogger", "get_correlation_id"]


CORRELATION_ID_HEADER = b"x-correlation-id"


def get_view_name(request: Request) -> Optional[str]:
    try:
        view_name = request.scope["route"].name
    except KeyError:
        return None

    return view_name


def is_health_check(request: Request) -> bool:
    return get_view_name(request) == "health_check"


def get_correlation_id(request: Request) -> Optional[UUID]:
    headers = dict(request.scope["headers"])
    try:
        return UUID(headers[CORRELATION_ID_HEADER].decode())
    except (KeyError, ValueError, UnicodeDecodeError):
        return None


def ensure_correlation_id(request: Request) -> None:
    correlation_id = get_correlation_id(request)
    if correlation_id is None:
        # generate an id and update the request inplace
        correlation_id = uuid4()
        headers = dict(request.scope["headers"])
        headers[CORRELATION_ID_HEADER] = str(correlation_id).encode()
        request.scope["headers"] = list(headers.items())


class FastAPIAccessLogger:
    def __init__(self, hostname: str, gateway_override: Optional[Gateway] = None):
        self.origin = f"{hostname}-{os.getpid()}"
        self.gateway_override = gateway_override

    @property
    def gateway(self) -> Gateway:
        return self.gateway_override or inject.instance(FluentbitGateway)

    async def __call__(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.scope["type"] != "http" or is_health_check(request):
            return await call_next(request)

        ensure_correlation_id(request)

        time_received = time.time()
        response = await call_next(request)
        request_time = time.time() - time_received

        # Instead of logging directly, set it as background task so that it is
        # executed after the response. See https://www.starlette.io/background/.
        if response.background is None:
            response.background = BackgroundTasks()
        response.background.add_task(
            log_access,
            self.gateway,
            request,
            response,
            time_received,
            request_time,
        )
        return response


async def log_access(
    gateway: Gateway,
    request: Request,
    response: Response,
    time_received: float,
    request_time: float,
) -> None:
    """
    Create a dictionary with logging data.
    """
    try:
        content_length = int(response.headers.get("content-length"))
    except (TypeError, ValueError):
        content_length = None

    item = {
        "tag_suffix": "access_log",
        "remote_address": getattr(request.client, "host", None),
        "method": request.method,
        "path": request.url.path,
        "portal": request.url.netloc,
        "referer": request.headers.get("referer"),
        "user_agent": request.headers.get("user-agent"),
        "query_params": request.url.query,
        "view_name": get_view_name(request),
        "status": response.status_code,
        "content_type": response.headers.get("content-type"),
        "content_length": content_length,
        "time": time_received,
        "request_time": request_time,
        "correlation_id": str(get_correlation_id(request)),
    }
    await gateway.add(item)
