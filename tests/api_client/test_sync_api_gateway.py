from unittest import mock

import pytest

from clean_python.api_client import SyncApiGateway
from clean_python.api_client import SyncApiProvider

MODULE = "clean_python.api_client.api_provider"


class TstSyncApiGateway(SyncApiGateway, path="foo/{id}"):
    pass


@pytest.fixture
def api_provider():
    return mock.MagicMock(spec_set=SyncApiProvider)


@pytest.fixture
def api_gateway(api_provider) -> SyncApiGateway:
    return TstSyncApiGateway(api_provider)


def test_get(api_gateway: SyncApiGateway):
    actual = api_gateway.get(14)

    api_gateway.provider.request.assert_called_once_with("GET", "foo/14")
    assert actual is api_gateway.provider.request.return_value


def test_add(api_gateway: SyncApiGateway):
    actual = api_gateway.add({"foo": 2})

    api_gateway.provider.request.assert_called_once_with(
        "POST", "foo/", json={"foo": 2}
    )
    assert actual is api_gateway.provider.request.return_value


def test_remove(api_gateway: SyncApiGateway):
    actual = api_gateway.remove(2)

    api_gateway.provider.request.assert_called_once_with("DELETE", "foo/2")
    assert actual is True


def test_remove_does_not_exist(api_gateway: SyncApiGateway):
    api_gateway.provider.request.return_value = None
    actual = api_gateway.remove(2)
    assert actual is False
