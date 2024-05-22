from typing import Iterator
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from fastapi.security import SecurityScopes

from clean_python import PermissionDenied
from clean_python.fastapi import AuthSettings
from clean_python.fastapi import default_scope_verifier
from clean_python.fastapi.security import OAuth2Schema
from clean_python.oauth2 import OAuth2Settings
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifierSettings


@pytest.fixture
def token() -> Token:
    return Token(claims={"sub": "abc123", "scope": "a b", "username": "foo"})


@pytest.fixture
def token_multitenant() -> Token:
    return Token(
        claims={
            "sub": "abc123",
            "scope": "a b",
            "username": "foo",
            "tenant": 1,
            "tenant_name": "bar",
        }
    )


@pytest.mark.parametrize("scope", [["a"], ["b"], ["a", "b"]])
async def test_default_scope_verifier(token, scope):
    default_scope_verifier(None, scope, token)


@pytest.mark.parametrize("scope", [["c"], ["a", "c"]])
async def test_default_scope_verifier_err(token, scope):
    with pytest.raises(PermissionDenied):
        default_scope_verifier(None, scope, token)


@pytest.fixture
def token_verifier_cls() -> Iterator[Mock]:
    with patch("clean_python.fastapi.security.TokenVerifier") as m:
        yield m


@pytest.fixture
def auth(token_verifier_cls) -> AuthSettings:
    return AuthSettings(
        token=TokenVerifierSettings(issuer="some-issuer"),
        oauth2=OAuth2Settings(
            authorization_url="http://testserver/authorize",
            token_url="http://testserver/token",
        ),
    )


@pytest.fixture
def auth_scheme(auth: AuthSettings) -> OAuth2Schema:
    return OAuth2Schema(auth)


def test_auth_scheme_init(
    auth_scheme: OAuth2Schema, auth: AuthSettings, token_verifier_cls: Mock
):
    token_verifier_cls.assert_called_once_with(auth.token)

    assert auth_scheme._verifier is token_verifier_cls.return_value
    assert auth_scheme._scope_verifier is default_scope_verifier

    assert auth_scheme.scheme_name == "OAuth2"
    assert auth_scheme.model.flows.authorizationCode.tokenUrl == auth.oauth2.token_url
    assert (
        auth_scheme.model.flows.authorizationCode.authorizationUrl
        == auth.oauth2.authorization_url
    )
    assert auth_scheme.model.flows.authorizationCode.scopes == auth.oauth2.scopes


async def test_auth_scheme_call(auth_scheme: OAuth2Schema, token_verifier_cls: Mock):
    auth_scheme._scope_verifier = Mock()

    request = Mock()
    request.headers = {"Authorization": "blabla"}

    token = Mock()
    token.user == "user1"
    token.tenant = "tenant1"

    token_verifier_cls.return_value.return_value = token

    actual = await auth_scheme(request, SecurityScopes(["foo", "bar"]))

    token_verifier_cls.return_value.assert_called_once_with("blabla")
    assert actual is token_verifier_cls.return_value.return_value
    auth_scheme._scope_verifier.assert_called_once_with(
        request, frozenset(["foo", "bar"]), token
    )
