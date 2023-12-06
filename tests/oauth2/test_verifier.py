# (c) Nelen & Schuurmans

import json
import socket
import time
import urllib.request
from io import BytesIO
from unittest import mock

import jwt
import pytest

from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifier


@pytest.fixture
def patched_verifier(settings, jwk_patched):
    return TokenVerifier(settings)


def test_verifier_ok(patched_verifier, token_generator, jwk_patched):
    token = token_generator()
    verified_token = patched_verifier("Bearer " + token)

    assert isinstance(verified_token, Token)
    assert verified_token.user.id == "foo"
    assert verified_token.tenant is None
    assert verified_token.scope == {"user"}

    jwk_patched.assert_called_once_with(
        "https://some/auth/server/.well-known/jwks.json"
    )


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
        {"username": None},
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


@mock.patch.object(urllib.request, "urlopen")
def test_get_key_timeout(urlopen, patched_verifier, token_generator, public_key):
    def side_effect():
        assert socket.getdefaulttimeout() == 0.1
        return BytesIO(json.dumps({"keys": [public_key]}).encode())

    urlopen.return_value.__enter__.side_effect = side_effect

    assert socket.getdefaulttimeout() is None
    key = patched_verifier.get_key(token_generator(), timeout=0.1)
    assert socket.getdefaulttimeout() is None

    assert isinstance(key, jwt.PyJWK)
    assert key.key_id == public_key["kid"]


@mock.patch.object(urllib.request, "urlopen")
def test_get_key_invalid_kid(urlopen, settings, token_generator, public_key):
    urlopen.return_value.__enter__.return_value = BytesIO(
        json.dumps({"keys": []}).encode()
    )

    with pytest.raises(jwt.exceptions.PyJWTError):
        TokenVerifier(settings).get_key(token_generator())
