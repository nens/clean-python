from http import HTTPStatus
from unittest import mock

import pytest

from clean_python import DoesNotExist
from clean_python import Json
from clean_python import Mapper
from clean_python.api_client import ApiException
from clean_python.api_client import ApiGateway
from clean_python.api_client import ApiProvider


class TstApiGateway(ApiGateway, path="foo/{id}"):
    pass


@pytest.fixture
def api_provider():
    return mock.MagicMock(spec_set=ApiProvider)


@pytest.fixture
def api_gateway(api_provider) -> ApiGateway:
    return TstApiGateway(api_provider)


async def test_get(api_gateway: ApiGateway):
    actual = await api_gateway.get(14)

    api_gateway.provider.request.assert_called_once_with("GET", "foo/14")
    assert actual is api_gateway.provider.request.return_value


async def test_add(api_gateway: ApiGateway):
    actual = await api_gateway.add({"foo": 2})

    api_gateway.provider.request.assert_called_once_with(
        "POST", "foo/", json={"foo": 2}
    )
    assert actual is api_gateway.provider.request.return_value


async def test_remove(api_gateway: ApiGateway):
    actual = await api_gateway.remove(2)

    api_gateway.provider.request.assert_called_once_with("DELETE", "foo/2")
    assert actual is True


async def test_remove_does_not_exist(api_gateway: ApiGateway):
    api_gateway.provider.request.side_effect = ApiException(
        {}, status=HTTPStatus.NOT_FOUND
    )
    actual = await api_gateway.remove(2)
    assert actual is False


async def test_update(api_gateway: ApiGateway):
    actual = await api_gateway.update({"id": 2, "foo": "bar"})

    api_gateway.provider.request.assert_called_once_with(
        "PATCH", "foo/2", json={"foo": "bar"}
    )
    assert actual is api_gateway.provider.request.return_value


async def test_update_no_id(api_gateway: ApiGateway):
    with pytest.raises(DoesNotExist):
        await api_gateway.update({"foo": "bar"})

    assert not api_gateway.provider.request.called


async def test_update_does_not_exist(api_gateway: ApiGateway):
    api_gateway.provider.request.side_effect = ApiException(
        {}, status=HTTPStatus.NOT_FOUND
    )
    with pytest.raises(DoesNotExist):
        await api_gateway.update({"id": 2, "foo": "bar"})


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


class TstMappedApiGateway(ApiGateway, path="foo/{id}"):
    mapper = TstMapper()


@pytest.fixture
def mapped_api_gateway(api_provider) -> ApiGateway:
    return TstMappedApiGateway(api_provider)


async def test_get_with_mapper(mapped_api_gateway: ApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 14, "name": "FOO"}

    assert await mapped_api_gateway.get(14) == {"id": 14, "name": "foo"}


async def test_add_with_mapper(mapped_api_gateway: ApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 3, "name": "FOO"}

    assert await mapped_api_gateway.add({"name": "foo"}) == {"id": 3, "name": "foo"}

    mapped_api_gateway.provider.request.assert_called_once_with(
        "POST", "foo/", json={"name": "FOO"}
    )


async def test_update_with_mapper(mapped_api_gateway: ApiGateway):
    mapped_api_gateway.provider.request.return_value = {"id": 2, "name": "BAR"}

    assert await mapped_api_gateway.update({"id": 2, "name": "bar"}) == {
        "id": 2,
        "name": "bar",
    }

    mapped_api_gateway.provider.request.assert_called_once_with(
        "PATCH", "foo/2", json={"name": "BAR"}
    )
