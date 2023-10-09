import pytest

from clean_python.oauth2.client_credentials import CCTokenGateway
from clean_python.oauth2.client_credentials import is_token_usable
from clean_python.oauth2.client_credentials import OAuth2CCSettings
from clean_python.oauth2.client_credentials import SyncCCTokenGateway


@pytest.fixture
def settings(fastapi_example_app) -> OAuth2CCSettings:
    # these settings match those hardcoded in the example app
    return OAuth2CCSettings(
        token_url=fastapi_example_app + "/v1/token",
        client_id="testclient",
        client_secret="supersecret",
        scope="all",
    )


@pytest.fixture
def gateway(settings) -> CCTokenGateway:
    return CCTokenGateway(settings)


async def test_fetch_token(gateway: CCTokenGateway):
    response = await gateway._fetch_token()
    assert is_token_usable(response, 0)


@pytest.fixture
def sync_gateway(settings) -> SyncCCTokenGateway:
    return SyncCCTokenGateway(settings)


def test_fetch_token_sync(sync_gateway: SyncCCTokenGateway):
    response = sync_gateway._fetch_token()
    assert is_token_usable(response, 0)
