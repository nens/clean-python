from http import HTTPStatus

import pytest
import yaml
from fastapi.testclient import TestClient

from clean_python import ctx
from clean_python import InMemoryGateway
from clean_python import Json
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v


class FooResource(Resource, version=v(1), name="testing"):
    @get("/foo", summary="foo endpoint, for testing")
    def foo_get(self):
        return {
            "path": str(ctx.path),
            "user": ctx.user,
            "tenant": ctx.tenant,
            "correlation_id": str(ctx.correlation_id),
        }


@pytest.fixture
def expected_schema() -> Json:
    return {
        "info": {"description": "testing", "title": "test", "version": "v1"},
        "openapi": "3.1.0",
        "paths": {
            "/foo": {
                "get": {
                    "operationId": "foo_get",
                    "responses": {
                        "200": {
                            "content": {"application/json": {"schema": {}}},
                            "description": "Successful Response",
                        },
                        "400": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ValidationErrorResponse"
                                    }
                                }
                            },
                            "description": "Bad Request",
                        },
                        "default": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/DefaultErrorResponse"
                                    }
                                }
                            },
                            "description": "Default Response",
                        },
                    },
                    # "security": [
                    #     {"OAuth2Bearer": []},
                    #     {"OAuth2Bearer": []},
                    #     {"OAuth2Bearer": []},
                    # ],
                    "summary": "foo endpoint, for testing",
                    "tags": ["testing"],
                }
            }
        },
        "components": {
            "schemas": {
                "DefaultErrorResponse": {
                    "properties": {
                        "detail": {
                            "anyOf": [{"type": "string"}, {"type": "null"}],
                            "title": "Detail",
                        },
                        "message": {"title": "Message", "type": "string"},
                    },
                    "required": ["message", "detail"],
                    "title": "DefaultErrorResponse",
                    "type": "object",
                },
                "ValidationErrorEntry": {
                    "properties": {
                        "loc": {
                            "items": {
                                "anyOf": [{"type": "string"}, {"type": "integer"}]
                            },
                            "title": "Loc",
                            "type": "array",
                        },
                        "msg": {"title": "Msg", "type": "string"},
                        "type": {"title": "Type", "type": "string"},
                    },
                    "required": ["loc", "msg", "type"],
                    "title": "ValidationErrorEntry",
                    "type": "object",
                },
                "ValidationErrorResponse": {
                    "properties": {
                        "detail": {
                            "items": {
                                "$ref": "#/components/schemas/ValidationErrorEntry"
                            },
                            "title": "Detail",
                            "type": "array",
                        },
                        "message": {"title": "Message", "type": "string"},
                    },
                    "required": ["message", "detail"],
                    "title": "ValidationErrorResponse",
                    "type": "object",
                },
            },
            "securitySchemes": {
                "OAuth2Bearer": {
                    "bearerFormat": "JWT",
                    "scheme": "bearer",
                    "type": "http",
                }
            },
        },
    }


@pytest.fixture
def app():
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        access_logger_gateway=InMemoryGateway([]),
    )


@pytest.fixture
def client(app):
    return TestClient(app)


def test_schema(client: TestClient, expected_schema: Json):
    response = client.get("v1/openapi.json")

    assert response.status_code == HTTPStatus.OK

    actual = response.json()

    for path in actual["paths"].values():
        for operation in path.values():
            del operation["security"]
    del actual["servers"]

    assert actual == expected_schema


def test_schema_yaml(client: TestClient, expected_schema: Json):
    response = client.get("v1/openapi.yaml")

    assert response.status_code == HTTPStatus.OK
    assert response.headers["content-type"] == "text/yaml; charset=utf-8"

    actual = yaml.safe_load(response.content.decode("utf-8"))

    for path in actual["paths"].values():
        for operation in path.values():
            del operation["security"]

    assert actual == expected_schema
