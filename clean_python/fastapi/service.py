# (c) Nelen & Schuurmans

from collections.abc import Callable
from contextlib import asynccontextmanager
from inspect import iscoroutinefunction
from typing import Any

from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import AnyHttpUrl
from pydantic import TypeAdapter
from starlette.types import ASGIApp
from starlette.types import StatelessLifespan

from clean_python import BadRequest
from clean_python import Conflict
from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Gateway
from clean_python import PermissionDenied
from clean_python import Unauthorized

from .error_responses import conflict_handler
from .error_responses import DefaultErrorResponse
from .error_responses import not_found_handler
from .error_responses import permission_denied_handler
from .error_responses import unauthorized_handler
from .error_responses import validation_error_handler
from .error_responses import ValidationErrorResponse
from .fastapi_access_logger import FastAPIAccessLogger
from .fastapi_access_logger import get_correlation_id
from .resource import APIVersion
from .resource import clean_resources
from .resource import Resource
from .schema import add_cached_openapi_yaml
from .security import AuthSettings
from .security import OAuth2Schema
from .security import set_auth_scheme

__all__ = ["Service"]


def get_swagger_ui_init_oauth(
    auth: AuthSettings | None = None,
) -> dict[str, Any] | None:
    return (
        None
        if auth is None or not auth.oauth2.login_enabled()
        else {
            "clientId": auth.oauth2.client_id,
            "usePkceWithAuthorizationCodeGrant": True,
        }
    )


AnyHttpUrlTA = TypeAdapter(AnyHttpUrl)


async def set_request_context(request: Request) -> None:
    ctx.path = AnyHttpUrlTA.validate_python(str(request.url))
    ctx.correlation_id = get_correlation_id(request)


async def health_check():
    """Simple health check route"""
    return {"health": "OK"}


async def _maybe_await(func: Callable[[], Any]) -> None:
    if iscoroutinefunction(func):
        await func()
    else:
        func()


def to_lifespan(
    on_startup: list[Callable[[], Any]],
    on_shutdown: list[Callable[[], Any]],
) -> StatelessLifespan[ASGIApp] | None:
    @asynccontextmanager
    async def lifespan(app: ASGIApp):
        for func in on_startup:
            await _maybe_await(func)
        yield
        for func in on_shutdown:
            await _maybe_await(func)

    return lifespan


class Service:
    resources: list[Resource]

    def __init__(self, *args: Resource):
        self.resources = clean_resources(args)

    @property
    def versions(self) -> set[APIVersion]:
        return {x.version for x in self.resources}

    def _create_root_app(
        self,
        title: str,
        description: str,
        hostname: str,
        on_startup: list[Callable[[], Any]] | None = None,
        on_shutdown: list[Callable[[], Any]] | None = None,
        access_logger_gateway: Gateway | None = None,
    ) -> FastAPI:
        app = FastAPI(
            title=title,
            description=description,
            lifespan=to_lifespan(on_startup or [], on_shutdown or []),
            servers=[
                {"url": f"{x.prefix}", "description": x.description}
                for x in self.versions
            ],
            root_path_in_servers=False,
        )
        if access_logger_gateway is not None:
            app.middleware("http")(
                FastAPIAccessLogger(hostname=hostname, gateway=access_logger_gateway)
            )
        app.get("/health", include_in_schema=False)(health_check)
        return app

    def _create_versioned_app(
        self, version: APIVersion, auth_scheme: OAuth2Schema | None, **fastapi_kwargs
    ) -> FastAPI:
        resources = [x for x in self.resources if x.version == version]
        app = FastAPI(
            version=version.prefix,
            tags=sorted(
                [x.get_openapi_tag().model_dump() for x in resources],
                key=lambda x: x["name"],
            ),
            **fastapi_kwargs,
        )
        for resource in resources:
            app.include_router(
                resource.get_router(
                    version,
                    responses={
                        "400": {"model": ValidationErrorResponse},
                        "default": {"model": DefaultErrorResponse},
                    },
                    auth_scheme=auth_scheme,
                )
            )
        app.add_exception_handler(DoesNotExist, not_found_handler)
        app.add_exception_handler(Conflict, conflict_handler)
        app.add_exception_handler(RequestValidationError, validation_error_handler)
        app.add_exception_handler(BadRequest, validation_error_handler)
        app.add_exception_handler(PermissionDenied, permission_denied_handler)
        app.add_exception_handler(Unauthorized, unauthorized_handler)
        add_cached_openapi_yaml(app)
        return app

    def create_app(
        self,
        title: str,
        description: str,
        hostname: str,
        auth: AuthSettings | None = None,
        on_startup: list[Callable[[], Any]] | None = None,
        on_shutdown: list[Callable[[], Any]] | None = None,
        access_logger_gateway: Gateway | None = None,
    ) -> ASGIApp:
        auth_scheme = set_auth_scheme(auth)
        app = self._create_root_app(
            title=title,
            description=description,
            hostname=hostname,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            access_logger_gateway=access_logger_gateway,
        )
        fastapi_kwargs = {
            "title": title,
            "description": description,
            "dependencies": [Depends(set_request_context)],
            "swagger_ui_init_oauth": get_swagger_ui_init_oauth(auth),
        }
        versioned_apps = {
            v: self._create_versioned_app(v, auth_scheme=auth_scheme, **fastapi_kwargs)
            for v in self.versions
        }
        for v, versioned_app in versioned_apps.items():
            app.mount("/" + v.prefix, versioned_app)
        return app
