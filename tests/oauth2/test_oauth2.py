# (c) Nelen & Schuurmans

import time

import pytest

from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.oauth2 import TokenVerifier


@pytest.fixture
def patched_verifier(jwk_patched, settings):
    return TokenVerifier(settings)


def test_verifier_ok(patched_verifier, token_generator):
    token = token_generator()
    verified_claims = patched_verifier("Bearer " + token)
    assert verified_claims.user.id == "foo"
    assert verified_claims.tenant is None
    assert verified_claims.scope == {"user"}

    patched_verifier.get_key.assert_called_once_with(token)


def test_verifier_ok_with_username(patched_verifier, token_generator):
    token = token_generator(username="sinterklaas")
    verified_claims = patched_verifier("Bearer " + token)
    assert verified_claims.user.name == "sinterklaas"


def test_verifier_ok_with_tenant(patched_verifier, token_generator):
    token = token_generator(tenant="15")
    verified_claims = patched_verifier("Bearer " + token)
    assert verified_claims.tenant.id == 15
    assert verified_claims.tenant.name == ""


def test_verifier_ok_with_tenant_and_name(patched_verifier, token_generator):
    token = token_generator(tenant=15, tenant_name="foo")
    verified_claims = patched_verifier("Bearer " + token)
    assert verified_claims.tenant.id == 15
    assert verified_claims.tenant.name == "foo"


def test_verifier_exp_leeway(patched_verifier, token_generator):
    token = token_generator(exp=int(time.time()) - 60)
    patched_verifier("Bearer " + token)


def test_verifier_multiple_scopes(patched_verifier, token_generator, settings):
    token = token_generator(scope=f"scope1 {settings.scope} scope3")
    patched_verifier("Bearer " + token)


@pytest.mark.parametrize(
    "claim_overrides",
    [
        {"iss": "https://authserver"},
        {"iss": None},
        {"scope": "nothing"},
        {"scope": None},
        {"exp": int(time.time()) - 3600},
        {"exp": None},
        {"nbf": int(time.time()) + 3600},
        {"token_use": "id"},
        {"token_use": None},
        {"sub": None},
    ],
)
def test_verifier_bad(patched_verifier, token_generator, claim_overrides):
    token = token_generator(**claim_overrides)
    with pytest.raises(Unauthorized):
        patched_verifier("Bearer " + token)


def test_verifier_authorize(patched_verifier, token_generator):
    token = token_generator(sub="bar")
    with pytest.raises(PermissionDenied):
        patched_verifier("Bearer " + token)


@pytest.mark.parametrize("prefix", ["", "foo ", "key ", "bearer ", "Bearer  "])
def test_verifier_bad_header_prefix(patched_verifier, token_generator, prefix):
    token = token_generator()
    with pytest.raises(Unauthorized):
        patched_verifier(prefix + token)


@pytest.mark.parametrize("header", ["", None, " "])
def test_verifier_no_header(patched_verifier, header):
    with pytest.raises(Unauthorized):
        patched_verifier(header)
