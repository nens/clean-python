# (c) Nelen & Schuurmans

from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from starlette.types import ASGIApp

from clean_python import BadRequest
from clean_python import Conflict
from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Gateway
from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.oauth2 import OAuth2SPAClientSettings
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifierSettings

from .error_responses import conflict_handler
from .error_responses import DefaultErrorResponse
from .error_responses import not_found_handler
from .error_responses import not_implemented_handler
from .error_responses import permission_denied_handler
from .error_responses import unauthorized_handler
from .error_responses import validation_error_handler
from .error_responses import ValidationErrorResponse
from .fastapi_access_logger import FastAPIAccessLogger
from .fastapi_access_logger import get_correlation_id
from .resource import APIVersion
from .resource import clean_resources
from .resource import Resource
from .security import get_token
from .security import JWTBearerTokenSchema
from .security import OAuth2SPAClientSchema
from .security import set_verifier

__all__ = ["Service"]


def get_auth_kwargs(auth_client: Optional[OAuth2SPAClientSettings]) -> Dict[str, Any]:
    if auth_client is None:
        return {
            "dependencies": [Depends(JWTBearerTokenSchema()), Depends(set_context)],
        }
    else:
        return {
            "dependencies": [
                Depends(OAuth2SPAClientSchema(client=auth_client)),
                Depends(set_context),
            ],
            "swagger_ui_init_oauth": {
                "clientId": auth_client.client_id,
                "usePkceWithAuthorizationCodeGrant": True,
            },
        }


async def set_context(
    request: Request,
    token: Token = Depends(get_token),
) -> None:
    ctx.path = request.url
    ctx.user = token.user
    ctx.tenant = token.tenant
    ctx.correlation_id = get_correlation_id(request)


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
        set_verifier(auth)
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
            **get_auth_kwargs(auth_client),
        }
        versioned_apps = {
            v: self._create_versioned_app(v, **kwargs) for v in self.versions
        }
        for v, versioned_app in versioned_apps.items():
            app.mount("/" + v.prefix, versioned_app)
        return app
