import logging
from typing import Any, Callable, Dict, List, Optional, Set

from asgiref.sync import sync_to_async
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.security import OAuth2AuthorizationCodeBearer
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from starlette.types import ASGIApp

from .context import RequestMiddleware
from .error_responses import (
    BadRequest,
    conflict_handler,
    DefaultErrorResponse,
    not_found_handler,
    not_implemented_handler,
    permission_denied_handler,
    unauthorized_handler,
    validation_error_handler,
    ValidationErrorResponse,
)
from clean_python.base.domain.exceptions import Conflict, DoesNotExist, PermissionDenied, Unauthorized
from .fastapi_access_logger import FastAPIAccessLogger
from clean_python.base.infrastructure.gateway import Gateway
from clean_python.oauth2.oauth2 import OAuth2AccessTokenVerifier, OAuth2Settings
from .resource import APIVersion, clean_resources, Resource

logger = logging.getLogger(__name__)


class OAuth2Dependable(OAuth2AuthorizationCodeBearer):
    """A fastapi 'dependable' configuring OAuth2.

    This does two things:
    - Verify the token in each request
    - (through FastAPI magic) add the scheme to the OpenAPI spec
    """

    def __init__(self, scope, settings: OAuth2Settings):
        self.verifier = sync_to_async(
            OAuth2AccessTokenVerifier(
                scope,
                issuer=settings.issuer,
                resource_server_id=settings.resource_server_id,
                algorithms=settings.algorithms,
                admin_users=settings.admin_users,
            ),
            thread_sensitive=False,
        )
        super().__init__(
            authorizationUrl=settings.authorization_url,
            tokenUrl=settings.token_url,
            scopes={
                f"{settings.resource_server_id}*:readwrite": "Full read/write access"
            },
        )

    async def __call__(self, request: Request) -> None:
        token = await super().__call__(request)
        try:
            await self.verifier(token)
        except Unauthorized:
            raise HTTPException(status_code=HTTP_401_UNAUTHORIZED)
        except PermissionDenied:
            raise HTTPException(status_code=HTTP_403_FORBIDDEN)


def fastapi_oauth_kwargs(auth: Optional[OAuth2Settings]) -> Dict:
    if auth is None:
        return {}
    return {
        "dependencies": [Depends(OAuth2Dependable(scope="*:readwrite", settings=auth))],
        "swagger_ui_init_oauth": {
            "clientId": auth.client_id,
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
                [x.get_openapi_tag().dict() for x in resources], key=lambda x: x["name"]
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
        auth: Optional[OAuth2Settings] = None,
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
            **fastapi_oauth_kwargs(auth),
        }
        versioned_apps = {
            v: self._create_versioned_app(v, **kwargs) for v in self.versions
        }
        for v, versioned_app in versioned_apps.items():
            app.mount("/" + v.prefix, versioned_app)
        return app
