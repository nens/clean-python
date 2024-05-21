import pytest

from clean_python import PermissionDenied
from clean_python.fastapi import default_scope_verifier
from clean_python.oauth2 import Token


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
    default_scope_verifier(token, scope)


@pytest.mark.parametrize("scope", [["c"], ["a", "c"]])
async def test_default_scope_verifier_err(token, scope):
    with pytest.raises(PermissionDenied):
        default_scope_verifier(token, scope)
