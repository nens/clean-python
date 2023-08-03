# (c) Nelen & Schuurmans

from contextvars import ContextVar

from fastapi import Request

from ..oauth2 import Claims

__all__ = ["ctx", "RequestMiddleware"]


class Context:
    def __init__(self):
        self._request_value: ContextVar[Request] = ContextVar("request_value")
        self._claims_value: ContextVar[Claims] = ContextVar("claims_value")

    @property
    def request(self) -> Request:
        return self._request_value.get()

    @request.setter
    def request(self, value: Request) -> None:
        self._request_value.set(value)

    @property
    def claims(self) -> Claims:
        return self._claims_value.get()

    @claims.setter
    def claims(self, value: Claims) -> None:
        self._claims_value.set(value)


ctx = Context()


class RequestMiddleware:
    """Save the current request in a context variable.

    We were experiencing database connections piling up until PostgreSQL's
    max_connections was hit, which has to do with BaseHTTPMiddleware not
    interacting properly with context variables. For more details, see:
    https://github.com/tiangolo/fastapi/issues/4719. Writing this
    middleware as generic ASGI middleware fixes the problem.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            ctx.request = Request(scope, receive)
        await self.app(scope, receive, send)
