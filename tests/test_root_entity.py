from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest

from clean_python import RootEntity
from clean_python.base.domain.exceptions import BadRequest

SOME_DATETIME = datetime(2023, 1, 1, tzinfo=timezone.utc)


class User(RootEntity):
    name: str


@pytest.fixture
def user():
    return User(
        id=4,
        name="jan",
        created_at=datetime(2010, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )


@pytest.fixture
def patched_now():
    with mock.patch(
        "clean_python.base.domain.root_entity.now", return_value=SOME_DATETIME
    ):
        yield


def test_create(patched_now):
    obj = User.create(name="piet")

    assert obj.id is None
    assert obj.name == "piet"
    assert obj.created_at == SOME_DATETIME
    assert obj.updated_at == SOME_DATETIME


def test_create_with_id():
    obj = User.create(id=42, name="piet")

    assert obj.id == 42


def test_update(user, patched_now):
    actual = user.update(name="piet")

    assert actual is not user
    assert actual.name == "piet"
    assert actual.updated_at == SOME_DATETIME
    assert actual.created_at == datetime(2010, 1, 1, tzinfo=timezone.utc)


def test_update_including_id(user):
    actual = user.update(id=4, name="piet")

    assert actual is not user
    assert actual.name == "piet"


@pytest.mark.parametrize("new_id", [None, 42, "foo"])
def test_update_with_wrong_id(user, new_id):
    with pytest.raises(BadRequest):
        user.update(id=new_id, name="piet")


@pytest.mark.parametrize("new_id", [None, 42])
def test_update_give_id(new_id):
    user_without_id = User.create(name="jan")
    actual = user_without_id.update(id=new_id, name="piet")

    assert actual.id == new_id
