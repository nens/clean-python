# This module is a copy paste of test_manage.py

from unittest import mock

import pytest

from clean_python import Conflict
from clean_python import Filter
from clean_python import RootEntity
from clean_python import SyncManage


class User(RootEntity):
    name: str


class ManageUser(SyncManage[User]):
    def __init__(self):
        self.repo = mock.Mock()


@pytest.fixture
def manage_user():
    return ManageUser()


def test_retrieve(manage_user):
    result = manage_user.retrieve(2)

    manage_user.repo.get.assert_called_with(2)
    assert result is manage_user.repo.get.return_value


def test_create(manage_user):
    result = manage_user.create({"name": "piet"})

    manage_user.repo.add.assert_called_once_with({"name": "piet"})

    assert result is manage_user.repo.add.return_value


def test_update(manage_user):
    result = manage_user.update(2, {"name": "jan"})

    manage_user.repo.update.assert_called_once_with(2, {"name": "jan"})

    assert result is manage_user.repo.update.return_value


def test_destroy(manage_user):
    result = manage_user.destroy(2)

    manage_user.repo.remove.assert_called_with(2)
    assert result is manage_user.repo.remove.return_value


def test_list(manage_user):
    result = manage_user.list()

    manage_user.repo.all.assert_called_once()
    assert result is manage_user.repo.all.return_value


def test_by(manage_user):
    result = manage_user.by("name", "piet")

    manage_user.repo.by.assert_called_with("name", "piet", params=None)
    assert result is manage_user.repo.by.return_value


def test_filter(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = manage_user.filter(filters)

    manage_user.repo.filter.assert_called_once_with(filters, params=None)

    assert result is manage_user.repo.filter.return_value


def test_count(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = manage_user.count(filters)

    manage_user.repo.count.assert_called_once_with(filters)

    assert result is manage_user.repo.count.return_value


def test_exists(manage_user):
    filters = [Filter(field="x", values=[1])]
    result = manage_user.exists(filters)

    manage_user.repo.exists.assert_called_once_with(filters)

    assert result is manage_user.repo.exists.return_value


@pytest.mark.parametrize("failure_count", [1, 2])
def test_update_retry_on_conflict(manage_user, failure_count: int):
    manage_user.repo.update.side_effect = (Conflict,) * failure_count + (
        {"name": "foo"},
    )

    result = manage_user.update(2, {"name": "jan"})

    assert manage_user.repo.update.call_count == failure_count + 1
    assert result == {"name": "foo"}


def test_update_retry_on_conflict_opt_out(manage_user):
    manage_user.repo.update.side_effect = (Conflict, {"name": "foo"})

    with pytest.raises(Conflict):
        manage_user.update(2, {"name": "jan"}, retry_on_conflict=False)
