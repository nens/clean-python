# This module is a copy-paste of test_gateway.py

from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import InMemorySyncGateway
from clean_python import PageOptions


@pytest.fixture
def in_memory_gateway():
    return InMemorySyncGateway(
        data=[
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3, "name": "c"},
        ]
    )


def test_get(in_memory_gateway):
    actual = in_memory_gateway.get(1)
    assert actual == in_memory_gateway.data[1]


def test_get_none(in_memory_gateway):
    actual = in_memory_gateway.get(4)
    assert actual is None


def test_add(in_memory_gateway):
    record = {"id": 5, "name": "d"}
    in_memory_gateway.add(record)
    assert in_memory_gateway.data[5] == record


def test_add_id_autoincrement(in_memory_gateway):
    record = {"name": "d"}
    in_memory_gateway.add(record)
    assert in_memory_gateway.data[4] == {"id": 4, "name": "d"}


def test_add_id_exists(in_memory_gateway):
    with pytest.raises(AlreadyExists):
        in_memory_gateway.add({"id": 3})


def test_update(in_memory_gateway):
    record = {"id": 3, "name": "d"}
    in_memory_gateway.update(record)
    assert in_memory_gateway.data[3] == record


def test_update_no_id(in_memory_gateway):
    with pytest.raises(DoesNotExist):
        in_memory_gateway.update({"no": "id"})


def test_update_does_not_exist(in_memory_gateway):
    with pytest.raises(DoesNotExist):
        in_memory_gateway.update({"id": 4})


def test_upsert(in_memory_gateway):
    record = {"id": 3, "name": "d"}
    in_memory_gateway.upsert(record)
    assert in_memory_gateway.data[3] == record


def test_upsert_no_id(in_memory_gateway):
    in_memory_gateway.upsert({"name": "x"})
    assert in_memory_gateway.data[4] == {"id": 4, "name": "x"}


def test_upsert_does_add(in_memory_gateway):
    in_memory_gateway.upsert({"id": 4, "name": "x"})
    assert in_memory_gateway.data[4] == {"id": 4, "name": "x"}


def test_remove(in_memory_gateway):
    assert in_memory_gateway.remove(1)
    assert 1 not in in_memory_gateway.data
    assert len(in_memory_gateway.data) == 2


def test_remove_not_existing(in_memory_gateway):
    assert not in_memory_gateway.remove(4)
    assert len(in_memory_gateway.data) == 3


def test_updated_if_unmodified_since(in_memory_gateway):
    existing = {"id": 4, "name": "e", "updated_at": datetime.now(timezone.utc)}
    new = {"id": 4, "name": "f", "updated_at": datetime.now(timezone.utc)}

    in_memory_gateway.add(existing)

    in_memory_gateway.update(new, if_unmodified_since=existing["updated_at"])
    assert in_memory_gateway.data[4]["name"] == "f"


@pytest.mark.parametrize(
    "if_unmodified_since", [datetime.now(timezone.utc), datetime(2010, 1, 1)]
)
def test_update_if_unmodified_since_not_ok(in_memory_gateway, if_unmodified_since):
    existing = {"id": 4, "name": "e", "updated_at": datetime.now(timezone.utc)}
    new = {"id": 4, "name": "f", "updated_at": datetime.now(timezone.utc)}

    in_memory_gateway.add(existing)
    with pytest.raises(Conflict):
        in_memory_gateway.update(new, if_unmodified_since=if_unmodified_since)


def test_filter_all(in_memory_gateway):
    actual = in_memory_gateway.filter([])
    assert actual == sorted(in_memory_gateway.data.values(), key=lambda x: x["id"])


def test_filter_all_with_params(in_memory_gateway):
    actual = in_memory_gateway.filter(
        [], params=PageOptions(limit=2, offset=1, order_by="id", ascending=False)
    )
    assert [x["id"] for x in actual] == [2, 1]


def test_filter(in_memory_gateway):
    actual = in_memory_gateway.filter([Filter(field="name", values=["b"])])
    assert actual == [in_memory_gateway.data[2]]


def test_count_all(in_memory_gateway):
    actual = in_memory_gateway.count([])
    assert actual == 3


def test_count_with_filter(in_memory_gateway):
    actual = in_memory_gateway.count([Filter(field="name", values=["b"])])
    assert actual == 1


@mock.patch.object(InMemorySyncGateway, "update")
def test_update_transactional(update):
    record = {"id": 3, "name": "d", "updated_at": datetime(2010, 1, 1)}
    gateway = InMemorySyncGateway([record])
    gateway.update_transactional(3, lambda x: {"name": x["name"] + "x"})

    update.assert_called_once_with(
        {"name": "dx"}, if_unmodified_since=datetime(2010, 1, 1)
    )


def test_update_transactional_does_not_exist(in_memory_gateway):
    with pytest.raises(DoesNotExist):
        in_memory_gateway.update_transactional(5, lambda x: x)


def test_exists_all(in_memory_gateway):
    assert in_memory_gateway.exists([])


def test_exists_with_filter(in_memory_gateway):
    assert in_memory_gateway.exists([Filter(field="name", values=["b"])])


def test_exists_with_filter_not(in_memory_gateway):
    assert not in_memory_gateway.exists([Filter(field="name", values=["bb"])])
