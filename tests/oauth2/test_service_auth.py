from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

from clean_python import ctx
from clean_python import InMemoryGateway
from clean_python.fastapi import AuthSettings
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v
from clean_python.oauth2 import OAuth2Settings
from clean_python.oauth2 import TokenVerifierSettings


class FooResource(Resource, version=v(1), name="testing"):
    @get("/foo")
    def testing(self):
        return "ok"

    @get("/bar", scope="admin")
    def scoped(self):
        return "ok"

    @get("/bar2", scope=["admin", "user"])
    def two_scopes(self):
        return "ok"

    @get("/baz", public=True)
    def public(self):
        return "ok"

    @get("/context")
    def context(self):
        return {
            "path": str(ctx.path),
            "user": ctx.user,
            "tenant": ctx.tenant,
        }


@pytest.fixture
def app(settings: TokenVerifierSettings):
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        auth=AuthSettings(
            token=settings,
            oauth2=OAuth2Settings(
                token_url="https://server/token",
                authorization_url="https://server/authorize",
                scopes={"*": "All", "foo": "Only Foo"},
            ),
        ),
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
            "Authorization": "Bearer " + token_generator(tenant=2, tenant_name="bar")
        },
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {
        "path": "http://testserver/v1/context",
        "user": {"id": "foo", "name": "piet"},
        "tenant": {"id": 2, "name": "bar"},
    }
    assert ctx.user.id != "foo"
    assert ctx.tenant is None


@pytest.mark.usefixtures("jwk_patched")
def test_client_credentials_ok(app, client: TestClient, token_generator):
    response = client.get(
        app.url_path_for("v1/testing"),
        headers={
            "Authorization": "Bearer " + token_generator(username=None, client_id="foo")
        },
    )

    assert response.status_code == HTTPStatus.OK


def test_public_ok(app, client: TestClient, jwk_patched):
    response = client.get(app.url_path_for("v1/public"))
    assert response.status_code == HTTPStatus.OK
    assert not jwk_patched.called


def test_auth_security_schemes(app, client: TestClient):
    response = client.get("v1/openapi.json")

    assert response.status_code == HTTPStatus.OK
    schema = response.json()

    schemes = schema["components"]["securitySchemes"]

    assert schemes == {
        "OAuth2": {
            "type": "oauth2",
            "flows": {
                "authorizationCode": {
                    "authorizationUrl": "https://server/authorize",
                    "scopes": {"*": "All", "foo": "Only Foo"},
                    "tokenUrl": "https://server/token",
                }
            },
        }
    }


def test_auth_security_scopes(client: TestClient):
    # https://github.com/OAI/OpenAPI-Specification/issues/287#issuecomment-76398547
    response = client.get("v1/openapi.json")

    assert response.status_code == HTTPStatus.OK
    schema = response.json()

    assert schema["paths"]["/bar"]["get"]["security"] == [{"OAuth2": ["admin"]}]

    # two scopes means: AND  https://github.com/OAI/OpenAPI-Specification/issues/287#issuecomment-76398547
    assert schema["paths"]["/bar2"]["get"]["security"] == [
        {"OAuth2": ["admin", "user"]}
    ]
