from functools import lru_cache
from io import StringIO

import yaml
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.openapi.docs import get_redoc_html
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse

OPENAPI_URL = "/openapi.json"
FAVICON_URL = "/favicon.ico"


def add_cached_openapi_yaml(app: FastAPI) -> None:
    @app.get(OPENAPI_URL.replace(".json", ".yaml"), include_in_schema=False)
    @lru_cache
    def openapi_yaml() -> Response:
        openapi_json = app.openapi()
        yaml_s = StringIO()
        yaml.dump(openapi_json, yaml_s)
        return Response(yaml_s.getvalue(), media_type="text/yaml")


def get_openapi_url(request: Request) -> str:
    root_path = request.scope.get("root_path", "").rstrip("/")
    return root_path + OPENAPI_URL


def add_swagger_ui(app: FastAPI, title: str, client_id: str | None) -> None:
    # Code below is copied from fastapi.applications to modify the favicon
    @app.get("/docs", include_in_schema=False)
    async def swagger_ui_html(request: Request) -> HTMLResponse:
        return get_swagger_ui_html(
            openapi_url=get_openapi_url(request),
            title=f"{title} - Swagger UI",
            swagger_favicon_url=FAVICON_URL,
            init_oauth={
                "clientId": client_id,
                "usePkceWithAuthorizationCodeGrant": True,
            }
            if client_id
            else None,
        )


def add_redoc(app: FastAPI, title: str) -> None:
    # Code below is copied from fastapi.applications to modify the favicon
    @app.get("/redoc", include_in_schema=False)
    async def redoc_html(request: Request) -> HTMLResponse:
        return get_redoc_html(
            openapi_url=get_openapi_url(request),
            title=f"{title} - ReDoc",
            redoc_favicon_url=FAVICON_URL,
        )
