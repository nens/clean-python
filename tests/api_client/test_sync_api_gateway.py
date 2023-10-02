from http import HTTPStatus
from unittest import mock

import pytest

from clean_python import DoesNotExist
from clean_python.api_client import ApiException
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
    api_gateway.provider.request.side_effect = ApiException(
        {}, status=HTTPStatus.NOT_FOUND
    )
    actual = api_gateway.remove(2)
    assert actual is False


def test_update(api_gateway: SyncApiGateway):
    actual = api_gateway.update({"id": 2, "foo": "bar"})

    api_gateway.provider.request.assert_called_once_with(
        "PATCH", "foo/2", json={"foo": "bar"}
    )
    assert actual is api_gateway.provider.request.return_value


def test_update_no_id(api_gateway: SyncApiGateway):
    with pytest.raises(DoesNotExist):
        api_gateway.update({"foo": "bar"})

    assert not api_gateway.provider.request.called


def test_update_does_not_exist(api_gateway: SyncApiGateway):
    api_gateway.provider.request.side_effect = ApiException(
        {}, status=HTTPStatus.NOT_FOUND
    )
    with pytest.raises(DoesNotExist):
        api_gateway.update({"id": 2, "foo": "bar"})
