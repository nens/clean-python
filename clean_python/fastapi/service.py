# (c) Nelen & Schuurmans

import logging
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Set

from asgiref.sync import sync_to_async
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.security import OAuth2AuthorizationCodeBearer
from starlette.types import ASGIApp

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Gateway
from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.oauth2 import OAuth2SPAClientSettings
from clean_python.oauth2 import TokenVerifier
from clean_python.oauth2 import TokenVerifierSettings

from .context import ctx
from .context import RequestMiddleware
from .error_responses import BadRequest
from .error_responses import conflict_handler
from .error_responses import DefaultErrorResponse
from .error_responses import not_found_handler
from .error_responses import not_implemented_handler
from .error_responses import permission_denied_handler
from .error_responses import unauthorized_handler
from .error_responses import validation_error_handler
from .error_responses import ValidationErrorResponse
from .fastapi_access_logger import FastAPIAccessLogger
from .resource import APIVersion
from .resource import clean_resources
from .resource import Resource

logger = logging.getLogger(__name__)

__all__ = ["Service"]


class OAuth2WithClientDependable(OAuth2AuthorizationCodeBearer):
    """A fastapi 'dependable' configuring OAuth2.

    This does two things:
    - Verify the token in each request
    - (through FastAPI magic) add the scheme to the OpenAPI spec
    """

    def __init__(
        self, settings: TokenVerifierSettings, client: OAuth2SPAClientSettings
    ):
        self.verifier = sync_to_async(TokenVerifier(settings), thread_sensitive=False)
        super().__init__(
            authorizationUrl=str(client.authorization_url),
            tokenUrl=str(client.token_url),
        )

    async def __call__(self, request: Request) -> None:
        ctx.claims = await self.verifier(request.headers.get("Authorization"))


class OAuth2WithoutClientDependable:
    """A fastapi 'dependable' configuring OAuth2.

    This does one thing:
    - Verify the token in each request
    """

    def __init__(self, settings: TokenVerifierSettings):
        self.verifier = sync_to_async(TokenVerifier(settings), thread_sensitive=False)

    async def __call__(self, request: Request) -> None:
        ctx.claims = await self.verifier(request.headers.get("Authorization"))


def get_auth_kwargs(
    auth: Optional[TokenVerifierSettings],
    auth_client: Optional[OAuth2SPAClientSettings],
) -> None:
    if auth is None:
        return {}
    if auth_client is None:
        return {
            "dependencies": [Depends(OAuth2WithoutClientDependable(settings=auth))],
        }
    else:
        return {
            "dependencies": [
                Depends(OAuth2WithClientDependable(settings=auth, client=auth_client))
            ],
            "swagger_ui_init_oauth": {
                "clientId": auth_client.client_id,
                "usePkceWithAuthorizationCodeGrant": True,
            },
        }


async def health_check():
    """Simple health check route"""
    return {"health": "OK"}


class Service:
    resources: List[Resource]

    def __init__(self, *args: Resource):
        self.resources = clean_resources(args)

    @property
    def versions(self) -> Set[APIVersion]:
        return set([x.version for x in self.resources])

    def _create_root_app(
        self,
        title: str,
        description: str,
        hostname: str,
        on_startup: Optional[List[Callable[[], Any]]] = None,
        access_logger_gateway: Optional[Gateway] = None,
    ) -> FastAPI:
        app = FastAPI(
            title=title,
            description=description,
            on_startup=on_startup,
            servers=[
                {"url": f"{x.prefix}", "description": x.description}
                for x in self.versions
            ],
            root_path_in_servers=False,
        )
        app.middleware("http")(
            FastAPIAccessLogger(
                hostname=hostname, gateway_override=access_logger_gateway
            )
        )
        app.add_middleware(RequestMiddleware)
        app.get("/health", include_in_schema=False)(health_check)
        return app

    def _create_versioned_app(self, version: APIVersion, **kwargs) -> FastAPI:
        resources = [x for x in self.resources if x.version == version]
        app = FastAPI(
            version=version.prefix,
            tags=sorted(
                [x.get_openapi_tag().model_dump() for x in resources],
                key=lambda x: x["name"],
            ),
            **kwargs,
        )
        for resource in resources:
            app.include_router(
                resource.get_router(
                    version,
                    responses={
                        "400": {"model": ValidationErrorResponse},
                        "default": {"model": DefaultErrorResponse},
                    },
                )
            )
        app.add_exception_handler(DoesNotExist, not_found_handler)
        app.add_exception_handler(Conflict, conflict_handler)
        app.add_exception_handler(RequestValidationError, validation_error_handler)
        app.add_exception_handler(BadRequest, validation_error_handler)
        app.add_exception_handler(NotImplementedError, not_implemented_handler)
        app.add_exception_handler(PermissionDenied, permission_denied_handler)
        app.add_exception_handler(Unauthorized, unauthorized_handler)
        return app

    def create_app(
        self,
        title: str,
        description: str,
        hostname: str,
        auth: Optional[TokenVerifierSettings] = None,
        auth_client: Optional[OAuth2SPAClientSettings] = None,
        on_startup: Optional[List[Callable[[], Any]]] = None,
        access_logger_gateway: Optional[Gateway] = None,
    ) -> ASGIApp:
        app = self._create_root_app(
            title=title,
            description=description,
            hostname=hostname,
            on_startup=on_startup,
            access_logger_gateway=access_logger_gateway,
        )
        kwargs = {
            "title": title,
            "description": description,
            **get_auth_kwargs(auth, auth_client),
        }
        versioned_apps = {
            v: self._create_versioned_app(v, **kwargs) for v in self.versions
        }
        for v, versioned_app in versioned_apps.items():
            app.mount("/" + v.prefix, versioned_app)
        return app
