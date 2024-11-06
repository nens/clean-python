from http import HTTPStatus
from uuid import UUID
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic_core import Url

from clean_python import ctx
from clean_python import InMemoryGateway
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v


class FooResource(Resource, version=v(1), name="testing"):
    @get("/context")
    def context(self):
        assert isinstance(ctx.path, Url)
        return {
            "path": str(ctx.path),
            "user": ctx.user,
            "tenant": ctx.tenant,
            "correlation_id": str(ctx.correlation_id),
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


def test_default_context(app, client: TestClient):
    response = client.get(app.url_path_for("v1/context"))

    assert response.status_code == HTTPStatus.OK

    body = response.json()

    assert body["path"] == "http://testserver/v1/context"
    UUID(body["correlation_id"])  # randomly generated uuid

    assert ctx.correlation_id is None


def test_x_correlation_id_header(app, client: TestClient):
    uid = str(uuid4())
    response = client.get(
        app.url_path_for("v1/context"),
        headers={"X-Correlation-Id": uid},
    )

    assert response.status_code == HTTPStatus.OK

    body = response.json()

    assert body["correlation_id"] == uid

    assert ctx.correlation_id is None
