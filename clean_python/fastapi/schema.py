from functools import lru_cache
from io import StringIO

import yaml
from fastapi import FastAPI
from fastapi import Response


def add_cached_openapi_yaml(app: FastAPI) -> None:
    @app.get("/openapi.yaml", include_in_schema=False)
    @lru_cache
    def openapi_yaml() -> Response:
        openapi_json = app.openapi()
        yaml_s = StringIO()
        yaml.dump(openapi_json, yaml_s)
        return Response(yaml_s.getvalue(), media_type="text/yaml")
