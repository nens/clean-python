# (c) Nelen & Schuurmans

from unittest import mock

import pytest

from clean_python import Conflict
from clean_python import Filter
from clean_python import Manage
from clean_python import RootEntity


class User(RootEntity):
    name: str


class ManageUser(Manage[User]):
    def __init__(self):
        self.repo = mock.AsyncMock()


@pytest.fixture
def manage_user():
    return ManageUser()


async def test_retrieve(manage_user):
    result = await manage_user.retrieve(2)

    manage_user.repo.get.assert_awaited_with(2)
    assert result is manage_user.repo.get.return_value


async def test_create(manage_user):
    result = await manage_user.create({"name": "piet"})

    manage_user.repo.add.assert_awaited_once_with({"name": "piet"})

    assert result is manage_user.repo.add.return_value


async def test_update(manage_user):
    result = await manage_user.update(2, {"name": "jan"})

    manage_user.repo.update.assert_awaited_once_with(2, {"name": "jan"})

    assert result is manage_user.repo.update.return_value


async def test_destroy(manage_user):
    result = await manage_user.destroy(2)

    manage_user.repo.remove.assert_awaited_with(2)
    assert result is manage_user.repo.remove.return_value


async def test_list(manage_user):
    result = await manage_user.list()

    manage_user.repo.all.assert_awaited_once()
    assert result is manage_user.repo.all.return_value


async def test_by(manage_user):
    result = await manage_user.by("name", "piet")

    manage_user.repo.by.assert_awaited_with("name", "piet", params=None)
    assert result is manage_user.repo.by.return_value


async def test_filter(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = await manage_user.filter(filters)

    manage_user.repo.filter.assert_awaited_once_with(filters, params=None)

    assert result is manage_user.repo.filter.return_value


async def test_count(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = await manage_user.count(filters)

    manage_user.repo.count.assert_awaited_once_with(filters)

    assert result is manage_user.repo.count.return_value


async def test_exists(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = await manage_user.exists(filters)

    manage_user.repo.exists.assert_awaited_once_with(filters)

    assert result is manage_user.repo.exists.return_value


@pytest.mark.parametrize("failure_count", [1, 2])
async def test_update_retry_on_conflict(manage_user, failure_count: int):
    manage_user.repo.update.side_effect = (Conflict,) * failure_count + (
        {"name": "foo"},
    )

    result = await manage_user.update(2, {"name": "jan"})

    assert manage_user.repo.update.call_count == failure_count + 1
    assert result == {"name": "foo"}


async def test_update_retry_on_conflict_opt_out(manage_user):
    manage_user.repo.update.side_effect = (Conflict, {"name": "foo"})

    with pytest.raises(Conflict):
        await manage_user.update(2, {"name": "jan"}, retry_on_conflict=False)
