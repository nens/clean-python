import base64
import json
import time
from unittest import mock

import pytest

from clean_python.oauth2.client_credentials import CCTokenGateway
from clean_python.oauth2.client_credentials import is_token_usable
from clean_python.oauth2.client_credentials import OAuth2CCSettings
from clean_python.oauth2.client_credentials import SyncCCTokenGateway

SECRET_KEY = "abcd1234"
MODULE = "clean_python.oauth2.client_credentials"


def get_token(claims: dict, expires_in: int = 3600) -> str:
    claims["exp"] = int(time.time()) + expires_in
    payload = base64.b64encode(json.dumps(claims).encode()).decode()
    return f"header.{payload}.signature"


@pytest.fixture
def settings() -> OAuth2CCSettings:
    return OAuth2CCSettings(
        client_id="cid",
        client_secret="secret",
        token_url="https://authserver/token",
        scope="all",
    )


@pytest.fixture
def gateway(settings) -> CCTokenGateway:
    with mock.patch(MODULE + ".ApiProvider", autospec=True):
        yield CCTokenGateway(settings)


@pytest.fixture
def sync_gateway(settings) -> SyncCCTokenGateway:
    with mock.patch(MODULE + ".SyncApiProvider", autospec=True):
        yield SyncCCTokenGateway(settings)


@pytest.mark.parametrize(
    "expires_in,leeway,expected",
    [
        (3600, 0, True),
        (-10, 0, False),
        (60, 300, False),
    ],
)
def test_is_token_usable(expires_in, leeway, expected):
    token = get_token({"user": "foo"}, expires_in=expires_in)
    assert is_token_usable(token, leeway) is expected


async def test_fetch_token(gateway: CCTokenGateway):
    gateway.provider.request.return_value = {"access_token": "foo"}

    token = await gateway._fetch_token()

    assert token == "foo"

    gateway.provider.request.assert_awaited_once_with(
        method="POST",
        path="",
        fields={"grant_type": "client_credentials", "scope": "all"},
        timeout=1.0,
    )


async def test_fetch_token_cache(gateway: CCTokenGateway):
    # empty cache: provider gets called
    token = get_token({})
    gateway.provider.request.return_value = {"access_token": token}
    actual = await gateway.fetch_token()
    assert actual == token
    assert gateway.provider.request.called

    gateway.provider.request.reset_mock()

    # cache is filled: provider is not called
    actual = await gateway.fetch_token()
    assert actual == token
    assert not gateway.provider.request.called

    gateway.provider.request.reset_mock()

    # token is not usable so it is refreshed:
    with mock.patch(MODULE + ".is_token_usable", side_effect=(False, True)):
        actual = await gateway.fetch_token()
        assert actual == token
        assert gateway.provider.request.called


def test_fetch_token_sync(sync_gateway: SyncCCTokenGateway):
    sync_gateway.provider.request.return_value = {"access_token": "foo"}

    token = sync_gateway._fetch_token()

    assert token == "foo"

    sync_gateway.provider.request.assert_called_once_with(
        method="POST",
        path="",
        fields={"grant_type": "client_credentials", "scope": "all"},
        timeout=1.0,
    )


def test_fetch_token_sync_cache(sync_gateway: SyncCCTokenGateway):
    # empty cache: provider gets called
    token = get_token({})
    sync_gateway.provider.request.return_value = {"access_token": token}
    actual = sync_gateway.fetch_token()
    assert actual == token
    assert sync_gateway.provider.request.called

    sync_gateway.provider.request.reset_mock()

    # cache is filled: provider is not called
    actual = sync_gateway.fetch_token()
    assert actual == token
    assert not sync_gateway.provider.request.called

    sync_gateway.provider.request.reset_mock()

    # token is not usable so it is refreshed:
    with mock.patch(MODULE + ".is_token_usable", side_effect=(False, True)):
        actual = sync_gateway.fetch_token()
        assert actual == token
        assert sync_gateway.provider.request.called
