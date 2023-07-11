# (c) Nelen & Schuurmans

import time
from unittest import mock

import jwt
import pytest

from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.oauth2 import OAuth2AccessTokenVerifier


@pytest.fixture
def settings():
    return {
        "issuer": "https://cognito-idp.region.amazonaws.com/region_abc123",
        "resource_server_id": "localhost/",
        "algorithms": ["RS256"],
        "admin_users": ["foo"],
    }


@pytest.fixture
def private_key():
    # this key was generated especially for this test suite; is has no other applications
    return {
        "p": "_PgJBxrGEy8I5KvY_nDRT9loaBPqHHn0AUiTa92zBrAX0qA8ZhV66pUkX2JehU3efduel4FOK2xx-W31p7kCLoaGsMtfKAPYC33KptCH9YXkeMQHq1jWfcRgAVXpdXc7M4pQxO8Dh2BU8qhtAzhpbP4tUPoLIGcTUGd-1ieDkqE",  # NOQA
        "kty": "RSA",
        "q": "hT0USPCNN4o2PauND53ubh2G5uOHzY9mfPuEXZ1fiRihCe5Bng0K8Pzx5QpSAjUY2-FhHa8jK8ITERmwT3MQKJpmlm_1R8GnaNVPOj8BpAhDlMzgkVikEGj0Pd7x_wdSko7KscyG-ZVsMw_KiCZpC6hMiI60w9GG14MtXhRVWhM",  # NOQA
        "d": "BNwTHorPcAMiDglxt5Ylz1jqQ67rYcnA0okvZxz0QPbLovuTM1WIaPIeGlqXNzB9NxXtZhHXtnhoSwPf2LxMmYWWgJLqhPQWRlqZhLhww0nGGUgk_b1gNnMQuuh2weLfPNUksddhDJHzW1pBiDQrhP0t064Pz_P8WtGUkBka5-Pb3pItaF_w4xDIhhTJS48kv5H-BrwK8Vlz-EofkmPgxXBvCwhVoXZihxEUVzc6X59e1UiymXr-3lbNeL-76Yb9JHJFjXh2o52v5eZDVT6ir-iUp7bBXTiZsFaBCUCfCjx3MiQkHNBNEV7Cr9DKvfGdK3r9IbkSAC1tiD4Y1oyZwQ",  # NOQA
        "e": "AQAB",
        "use": "sig",
        "kid": "_Lfex-skFCKBZd0xMN5dZSAX7uoG6LMx3i2qHReqU0c",
        "qi": "GNhYuNdxd4NyRhzreW72PWXzj2oIkm0rIHrcNW9bpqK1fxrsbiVUEVUly-cqpD_-AjFOyCWcKWQxHG7J8LeP2vW3_U4TLx_jKD9cc7S65gb37El1ihOwNWbapRxToOhP2sZa0g3y9P-M_8hQcfKr1OFMQMnD9wj-sVNw9yJf3I4",  # NOQA
        "dp": "xTs6BrEISEK-w1N9Dvy1JXWToroMKQGojiugzVQAVjGLkWvfS5RpzmZUAo52taZ911EZOHTXlqGpx1jFVGy5176JW2RlH5THqEX-b8tchcBL3yCv_hd4vHwUglYSfMRmgwvPZ4wXC0C_WqaYwA8Gm7UdbepWLIBRHbpjuOL8AaE",  # NOQA
        "alg": "RS256",
        "dq": "C4_UTcwKBRLKSCm10PAce5O2XBzMcQsLkrbkspbwbl4jw0_Yg9WP6H-aogx2N1jSMmppWgETpT1vGCHJietrMIrNcip-914Xn-I6wMws4UYSTzxEFHjDq-TfpOrOxxmkkbEwZ6Ne5xOPUxMAuTXUEb3l_keb6g4pjFQGwM405d8",  # NOQA
        "n": "g6k31kvFdTaCSxXhazC5JaVekYi836F0H_YLrDioQlwiegsGjUDYk5TM7z8iXwDIm0QZZgtoEBlEny8vXrt1WGMO8GGwnVNq0_ZAD3JYp-a_c0X7VM7I2Dze32zcy8mC4QhPedEbMVDzi1XrusGjNHWObkMKsLZ7RRlwdkgR4nRpzncou_2ZJLvc50C8tjd3juCpUMWXNsvDjoAenxoXs68SDK4h9QSjvaWaSHNRGYiYkGUvcL5rv3htbrHIUVAcBC9r0j5Ued1hBR9ND1KPxVJWnn8oRAxFrYIcQdaDFWnWdb5BY9pJQls9fHlt0PF9vXUm-GufWk0U8D4Lc8V78w",  # NOQA
    }


@pytest.fixture
def public_key(private_key):
    keys = ("alg", "e", "kid", "kty", "n", "use")
    return {k: private_key[k] for k in keys}


@pytest.fixture
def patched_verifier(public_key, settings):
    verifier = OAuth2AccessTokenVerifier(scope="all", **settings)
    with mock.patch.object(verifier, "jwk_client") as jwk_client:
        jwk_client.get_signing_key_from_jwt.return_value = jwt.PyJWK.from_dict(
            public_key
        )
        yield verifier


@pytest.fixture
def token_generator(private_key, settings):
    default_claims = {
        "sub": "foo",
        "iss": settings["issuer"],
        "scope": f"{settings['resource_server_id']}all",
        "token_use": "access",
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()) - 3600,
        "nbf": int(time.time()) - 3600,
    }

    def generate_token(**claim_overrides):
        claims = {**default_claims, **claim_overrides}
        claims = {k: v for (k, v) in claims.items() if v is not None}
        return jwt.encode(
            claims,
            key=jwt.PyJWK.from_dict(private_key).key,
            algorithm=private_key["alg"],
            headers={"kid": private_key["kid"]},
        )

    return generate_token


def test_verifier_ok(patched_verifier, token_generator):
    token = token_generator()
    verified_claims = patched_verifier(token)
    assert verified_claims == jwt.decode(token, options={"verify_signature": False})

    patched_verifier.jwk_client.get_signing_key_from_jwt.assert_called_once_with(token)


def test_verifier_exp_leeway(patched_verifier, token_generator):
    token = token_generator(exp=int(time.time()) - 60)
    patched_verifier(token)


def test_verifier_multiple_scopes(patched_verifier, token_generator, settings):
    token = token_generator(scope=f"scope1 {settings['resource_server_id']}all scope3")
    patched_verifier(token)


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
        patched_verifier(token)


def test_verifier_authorize(patched_verifier, token_generator):
    token = token_generator(sub="bar")
    with pytest.raises(PermissionDenied):
        patched_verifier(token)
