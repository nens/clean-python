from http import HTTPStatus
from unittest import mock

import pytest

from clean_python import DoesNotExist
from clean_python import Json
from clean_python import Mapper
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


class TstMapper(Mapper):
    def to_external(self, internal: Json) -> Json:
        result = {}
        if internal.get("id") is not None:
            result["id"] = internal["id"]
        if internal.get("name") is not None:
            result["name"] = internal["name"].upper()
        return result

    def to_internal(self, external: Json) -> Json:
        return {"id": external["id"], "name": external["name"].lower()}


class TstMappedSyncApiGateway(SyncApiGateway, path="foo/{id}"):
    mapper = TstMapper()


@pytest.fixture
def mapped_api_gateway(api_provider) -> SyncApiGateway:
    return TstMappedSyncApiGateway(api_provider)


def test_get_with_mapper(mapped_api_gateway: SyncApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 14, "name": "FOO"}

    assert mapped_api_gateway.get(14) == {"id": 14, "name": "foo"}


def test_add_with_mapper(mapped_api_gateway: SyncApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 3, "name": "FOO"}

    assert mapped_api_gateway.add({"name": "foo"}) == {"id": 3, "name": "foo"}

    mapped_api_gateway.provider.request.assert_called_once_with(
        "POST", "foo/", json={"name": "FOO"}
    )


def test_update_with_mapper(mapped_api_gateway: SyncApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 2, "name": "BAR"}

    assert mapped_api_gateway.update({"id": 2, "name": "bar"}) == {
        "id": 2,
        "name": "bar",
    }

    mapped_api_gateway.provider.request.assert_called_once_with(
        "PATCH", "foo/2", json={"name": "BAR"}
    )
