from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

from clean_python import ctx
from clean_python import InMemoryGateway
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v
from clean_python.oauth2 import OAuth2SPAClientSettings
from clean_python.oauth2 import TokenVerifierSettings


class FooResource(Resource, version=v(1), name="testing"):
    @get("/foo")
    def testing(self):
        return "ok"

    @get("/bar", scope="admin")
    def scoped(self):
        return "ok"

    @get("/context")
    def context(self):
        return {
            "path": str(ctx.path),
            "user": ctx.user,
            "tenant": ctx.tenant,
            "correlation_id": ctx.correlation_id,
        }


@pytest.fixture(params=["noclient", "client"])
def app(request, settings: TokenVerifierSettings):
    if request.param == "noclient":
        auth_client = None
    elif request.param == "client":
        auth_client = OAuth2SPAClientSettings(
            client_id="123",
            token_url="https://server/token",
            authorization_url="https://server/token",
        )
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        auth=settings,
        auth_client=auth_client,
        access_logger_gateway=InMemoryGateway([]),
    )


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.mark.usefixtures("jwk_patched")
def test_no_header(app, client: TestClient):
    response = client.get(app.url_path_for("v1/testing"))

    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.usefixtures("jwk_patched")
def test_ok(app, client: TestClient, token_generator):
    response = client.get(
        app.url_path_for("v1/testing"),
        headers={"Authorization": "Bearer " + token_generator()},
    )

    assert response.status_code == HTTPStatus.OK


@pytest.mark.usefixtures("jwk_patched")
def test_scoped_ok(app, client: TestClient, token_generator):
    response = client.get(
        app.url_path_for("v1/scoped"),
        headers={"Authorization": "Bearer " + token_generator(scope="user admin")},
    )

    assert response.status_code == HTTPStatus.OK


@pytest.mark.usefixtures("jwk_patched")
def test_scoped_forbidden(app, client: TestClient, token_generator):
    response = client.get(
        app.url_path_for("v1/scoped"),
        headers={"Authorization": "Bearer " + token_generator(scope="user")},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN


@pytest.mark.usefixtures("jwk_patched")
def test_context(app, client: TestClient, token_generator):
    response = client.get(
        app.url_path_for("v1/context"),
        headers={
            "Authorization": "Bearer " + token_generator(tenant=2, tenant_name="bar"),
            "X-Correlation-Id": "abc",
        },
    )

    assert response.status_code == HTTPStatus.OK

    body = response.json()

    assert body["path"] == "http://testserver/v1/context"
    assert body["user"] == {"id": "foo", "name": "piet"}
    assert body["tenant"] == {"id": 2, "name": "bar"}
    assert body["correlation_id"] == "abc"

    assert ctx.user.id != "foo"
    assert ctx.tenant is None
    assert ctx.correlation_id == ""
