from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

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


@pytest.fixture
def app(settings: TokenVerifierSettings):
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        auth=settings,
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


@pytest.fixture
def app2(settings: TokenVerifierSettings):
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        auth=settings,
        auth_client=OAuth2SPAClientSettings(
            client_id="123",
            token_url="https://server/token",
            authorization_url="https://server/token",
        ),
        access_logger_gateway=InMemoryGateway([]),
    )


@pytest.fixture
def client2(app):
    return TestClient(app)


@pytest.mark.usefixtures("jwk_patched")
def test_no_header2(app2, client2: TestClient):
    response = client2.get(app2.url_path_for("v1/testing"))

    assert response.status_code == HTTPStatus.UNAUTHORIZED


@pytest.mark.usefixtures("jwk_patched")
def test_ok2(app2, client2: TestClient, token_generator):
    response = client2.get(
        app2.url_path_for("v1/testing"),
        headers={"Authorization": "Bearer " + token_generator()},
    )

    assert response.status_code == HTTPStatus.OK
