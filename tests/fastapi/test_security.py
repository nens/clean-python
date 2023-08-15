import pytest

from clean_python import PermissionDenied
from clean_python.fastapi import requires_tenant
from clean_python.fastapi import requires_token
from clean_python.fastapi import requires_user
from clean_python.fastapi import RequiresScope
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


async def test_requires_token(token):
    assert await requires_token(token) is token


async def test_requires_token_err():
    with pytest.raises(PermissionDenied):
        await requires_token(None)


async def test_requires_user(token):
    assert await requires_user(token) == token.user


async def test_requires_tenant(token_multitenant):
    assert await requires_tenant(token_multitenant) == token_multitenant.tenant


async def test_requires_tenant_err(token):
    with pytest.raises(PermissionDenied):
        await requires_tenant(token)


@pytest.mark.parametrize("scope", ["a", "b"])
async def test_requires_scope(token, scope):
    await RequiresScope(scope)(token)


async def test_requires_scope_err(token):
    with pytest.raises(PermissionDenied):
        await RequiresScope("c")(token)


def test_requries_scope_no_spaces_allowed():
    with pytest.raises(AssertionError):
        RequiresScope("c ")
