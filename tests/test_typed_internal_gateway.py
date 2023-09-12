from typing import cast

import pytest
from pydantic import Field

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Id
from clean_python import InMemoryGateway
from clean_python import Json
from clean_python import Manage
from clean_python import Repository
from clean_python import RootEntity
from clean_python import TypedInternalGateway
from clean_python import ValueObject


# domain - other module
class User(RootEntity):
    name: str = Field(min_length=1)


class UserRepository(Repository[User]):
    pass


# application - other module
class ManageUser(Manage[User]):
    def __init__(self):
        self.repo = UserRepository(gateway=InMemoryGateway([]))

    async def update(self, id: Id, values: Json) -> User:
        if values.get("name") == "conflict":
            raise Conflict()
        return await self.repo.update(id, values)


# domain - this module
class UserObj(ValueObject):
    id: int
    name: str


# infrastructure - this module


class UserGateway(TypedInternalGateway[User, UserObj]):
    def __init__(self, manage: ManageUser):
        self._manage = manage

    @property
    def manage(self) -> ManageUser:
        return self._manage

    def _map(self, obj: User) -> UserObj:
        return UserObj(id=cast(int, obj.id), name=obj.name)


@pytest.fixture
def internal_gateway():
    return UserGateway(manage=ManageUser())


async def test_get_not_existing(internal_gateway: UserGateway):
    assert await internal_gateway.get(1) is None


async def test_add(internal_gateway: UserGateway):
    actual = await internal_gateway.add(UserObj(id=12, name="foo"))

    assert actual == UserObj(id=12, name="foo")


@pytest.fixture
async def internal_gateway_with_record(internal_gateway):
    await internal_gateway.add(UserObj(id=12, name="foo"))
    return internal_gateway


async def test_get(internal_gateway_with_record):
    assert await internal_gateway_with_record.get(12) == UserObj(id=12, name="foo")


async def test_filter(internal_gateway_with_record: UserGateway):
    assert await internal_gateway_with_record.filter([]) == [UserObj(id=12, name="foo")]


async def test_filter_2(internal_gateway_with_record: UserGateway):
    assert (
        await internal_gateway_with_record.filter([Filter(field="id", values=[1])])
        == []
    )


async def test_remove(internal_gateway_with_record: UserGateway):
    assert await internal_gateway_with_record.remove(12)

    assert internal_gateway_with_record.manage.repo.gateway.data == {}


async def test_remove_does_not_exist(internal_gateway: UserGateway):
    assert not await internal_gateway.remove(12)


async def test_add_bad_request(internal_gateway: UserGateway):
    # a 'bad request' should be reraised as a ValueError; errors in gateways
    # are an internal affair.
    with pytest.raises(ValueError):
        await internal_gateway.add(UserObj(id=12, name=""))


async def test_count(internal_gateway_with_record: UserGateway):
    assert await internal_gateway_with_record.count([]) == 1


async def test_count_2(internal_gateway_with_record: UserGateway):
    assert (
        await internal_gateway_with_record.count([Filter(field="id", values=[1])]) == 0
    )


async def test_exists(internal_gateway_with_record: UserGateway):
    assert await internal_gateway_with_record.exists([]) is True


async def test_exists_2(internal_gateway_with_record: UserGateway):
    assert (
        await internal_gateway_with_record.exists([Filter(field="id", values=[1])])
        is False
    )


async def test_update(internal_gateway_with_record):
    updated = await internal_gateway_with_record.update({"id": 12, "name": "bar"})

    assert updated == UserObj(id=12, name="bar")


@pytest.mark.parametrize(
    "values", [{"id": 12, "name": "bar"}, {"id": None, "name": "bar"}, {"name": "bar"}]
)
async def test_update_does_not_exist(internal_gateway, values):
    with pytest.raises(DoesNotExist):
        assert await internal_gateway.update(values)


async def test_update_bad_request(internal_gateway_with_record):
    # a 'bad request' should be reraised as a ValueError; errors in gateways
    # are an internal affair.
    with pytest.raises(ValueError):
        assert await internal_gateway_with_record.update({"id": 12, "name": ""})


async def test_update_conflict(internal_gateway_with_record):
    # a 'conflict' should bubble through the internal gateway
    with pytest.raises(Conflict):
        assert await internal_gateway_with_record.update({"id": 12, "name": "conflict"})
