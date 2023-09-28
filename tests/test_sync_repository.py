# This module is a copy paste of test_repository.py

from typing import List
from unittest import mock

import pytest

from clean_python import BadRequest
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import InMemorySyncGateway
from clean_python import Page
from clean_python import PageOptions
from clean_python import RootEntity
from clean_python import SyncRepository


class User(RootEntity):
    name: str


@pytest.fixture
def users():
    return [
        User.create(id=1, name="a"),
        User.create(id=2, name="b"),
        User.create(id=3, name="c"),
    ]


class UserSyncRepository(SyncRepository[User]):
    pass


@pytest.fixture
def user_repository(users: List[User]):
    return UserSyncRepository(
        gateway=InMemorySyncGateway(data=[x.model_dump() for x in users])
    )


@pytest.fixture
def page_options():
    return PageOptions(limit=10, offset=0, order_by="id")


def test_entity_attr(user_repository):
    assert user_repository.entity is User


def test_get(user_repository):
    actual = user_repository.get(1)
    assert actual.name == "a"


def test_get_does_not_exist(user_repository):
    with pytest.raises(DoesNotExist):
        user_repository.get(4)


@mock.patch.object(SyncRepository, "filter")
def test_all(filter_m, user_repository, page_options):
    filter_m.return_value = Page(total=0, items=[])
    assert user_repository.all(page_options) is filter_m.return_value

    filter_m.assert_called_once_with([], params=page_options)


def test_add(user_repository: UserSyncRepository):
    actual = user_repository.add(User.create(name="d"))
    assert actual.name == "d"
    assert user_repository.gateway.data[4] == actual.model_dump()


def test_add_json(user_repository: UserSyncRepository):
    actual = user_repository.add({"name": "d"})
    assert actual.name == "d"
    assert user_repository.gateway.data[4] == actual.model_dump()


def test_add_json_validates(user_repository: UserSyncRepository):
    with pytest.raises(BadRequest):
        user_repository.add({"id": "d"})


def test_update(user_repository: UserSyncRepository):
    actual = user_repository.update(id=2, values={"name": "d"})
    assert actual.name == "d"
    assert user_repository.gateway.data[2] == actual.model_dump()


def test_update_does_not_exist(user_repository: UserSyncRepository):
    with pytest.raises(DoesNotExist):
        user_repository.update(id=4, values={"name": "d"})


def test_update_validates(user_repository: UserSyncRepository):
    with pytest.raises(BadRequest):
        user_repository.update(id=2, values={"id": 6})


def test_remove(user_repository: UserSyncRepository):
    assert user_repository.remove(2)
    assert 2 not in user_repository.gateway.data


def test_remove_does_not_exist(user_repository: UserSyncRepository):
    assert not user_repository.remove(4)


def test_upsert_updates(user_repository: UserSyncRepository):
    actual = user_repository.upsert(User.create(id=2, name="d"))
    assert actual.name == "d"
    assert user_repository.gateway.data[2] == actual.model_dump()


def test_upsert_adds(user_repository: UserSyncRepository):
    actual = user_repository.upsert(User.create(id=4, name="d"))
    assert actual.name == "d"
    assert user_repository.gateway.data[4] == actual.model_dump()


@mock.patch.object(InMemorySyncGateway, "count")
def test_filter(count_m, user_repository: UserSyncRepository, users):
    actual = user_repository.filter([Filter(field="name", values=["b"])])
    assert actual == Page(total=1, items=[users[1]], limit=None, offest=None)
    assert not count_m.called


@mock.patch.object(InMemorySyncGateway, "count")
def test_filter_with_pagination(
    count_m, user_repository: UserSyncRepository, users, page_options
):
    actual = user_repository.filter([Filter(field="name", values=["b"])], page_options)
    assert actual == Page(
        total=1, items=[users[1]], limit=page_options.limit, offset=page_options.offset
    )
    assert not count_m.called


@pytest.mark.parametrize(
    "page_options",
    [
        PageOptions(limit=3, offset=0, order_by="id"),
        PageOptions(limit=10, offset=1, order_by="id"),
    ],
)
@mock.patch.object(InMemorySyncGateway, "count")
def test_filter_with_pagination_calls_count(
    count_m, user_repository: UserSyncRepository, users, page_options
):
    count_m.return_value = 123
    actual = user_repository.filter([], page_options)
    assert actual == Page(
        total=count_m.return_value,
        items=users[page_options.offset :],
        limit=page_options.limit,
        offset=page_options.offset,
    )
    assert count_m.called


@mock.patch.object(SyncRepository, "filter")
def test_by(filter_m, user_repository: UserSyncRepository, page_options):
    filter_m.return_value = Page(total=0, items=[])
    assert user_repository.by("name", "b", page_options) is filter_m.return_value

    filter_m.assert_called_once_with(
        [Filter(field="name", values=["b"])], params=page_options
    )


@mock.patch.object(InMemorySyncGateway, "count")
def test_count(gateway_count, user_repository):
    assert user_repository.count("foo") is gateway_count.return_value
    gateway_count.assert_called_once_with("foo")


@mock.patch.object(InMemorySyncGateway, "exists")
def test_exists(gateway_exists, user_repository):
    assert user_repository.exists("foo") is gateway_exists.return_value
    gateway_exists.assert_called_once_with("foo")
