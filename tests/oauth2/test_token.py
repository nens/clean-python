import pytest
from pydantic import ValidationError

from clean_python import Tenant
from clean_python import User
from clean_python.oauth2 import Token


@pytest.fixture
def claims():
    return {"sub": "abc123", "scope": "a b", "username": "foo"}


@pytest.fixture
def claims_multitenant():
    return {
        "sub": "abc123",
        "scope": "a b",
        "username": "foo",
        "tenant": 1,
        "tenant_name": "bar",
    }


def test_init(claims):
    Token(claims=claims)


def test_init_multitenant(claims_multitenant):
    Token(claims=claims_multitenant)


@pytest.mark.parametrize(
    "claims",
    [
        {"scope": "", "username": "foo"},
        {"sub": "abc123", "username": "foo"},
        {"sub": "abc123", "scope": ""},
        {"sub": "abc123", "scope": "", "username": "foo", "tenant": 1},
    ],
)
def test_init_err(claims):
    with pytest.raises(ValidationError):
        Token(claims=claims)


def test_user(claims):
    actual = Token(claims=claims).user
    assert actual == User(id="abc123", name="foo")


def test_scope(claims):
    actual = Token(claims=claims).scope
    assert actual == frozenset({"a", "b"})


def test_tenant(claims_multitenant):
    actual = Token(claims=claims_multitenant).tenant
    assert actual == Tenant(id=1, name="bar")


def test_no_tenant(claims):
    actual = Token(claims=claims).tenant
    assert actual is None
