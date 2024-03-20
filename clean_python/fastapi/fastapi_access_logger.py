# (c) Nelen & Schuurmans

import os
import time
from collections.abc import Awaitable
from collections.abc import Callable

from starlette.background import BackgroundTasks
from starlette.requests import Request
from starlette.responses import Response

from clean_python import Gateway

from .asgi import ensure_correlation_id
from .asgi import get_correlation_id
from .asgi import get_view_name
from .asgi import is_health_check

__all__ = ["FastAPIAccessLogger"]


class FastAPIAccessLogger:
    def __init__(self, hostname: str, gateway: Gateway):
        self.origin = f"{hostname}-{os.getpid()}"
        self.gateway = gateway

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
