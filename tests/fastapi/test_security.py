import pytest

from clean_python import PermissionDenied
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


@pytest.mark.parametrize("scope", ["a", "b"])
async def test_requires_scope(token, scope):
    await RequiresScope(scope)(token)


async def test_requires_scope_err(token):
    with pytest.raises(PermissionDenied):
        await RequiresScope("c")(token)


def test_requries_scope_no_spaces_allowed():
    with pytest.raises(AssertionError):
        RequiresScope("c ")
