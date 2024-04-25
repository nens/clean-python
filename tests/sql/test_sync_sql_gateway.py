from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Json
from clean_python import Mapper
from clean_python.sql import SyncSQLDatabase
from clean_python.sql import SyncSQLGateway

writer = Table(
    "writer",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", Text, nullable=False),
)


DT = datetime(2010, 1, 1, tzinfo=timezone.utc)


class TstMapper(Mapper):
    def to_external(self, internal: Json) -> Json:
        return {
            "id": internal.get("id"),
            "value": internal.get("name"),
        }

    def to_internal(self, external: Json) -> Json:
        return {
            "id": external["id"],
            "name": external["value"],
        }


class TstSQLGateway(SyncSQLGateway, table=writer):
    mapper = TstMapper()


@pytest.fixture
def sql_gateway() -> SyncSQLGateway:
    provider = mock.Mock(spec=SyncSQLDatabase)
    gateway = TstSQLGateway(provider)
    with mock.patch.object(gateway, "builder", autospec=True):
        yield gateway


def test_add(sql_gateway: SyncSQLGateway):
    records = [{"id": 2, "value": "foo"}]
    sql_gateway.provider.execute.return_value = records
    assert sql_gateway.add({"name": "foo"}) == {"id": 2, "name": "foo"}

    # query builder was called with mapped record
    sql_gateway.builder.insert.assert_called_once_with({"id": None, "value": "foo"})

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.insert.return_value
    )


@pytest.mark.parametrize("if_unmodified_since", [None, DT])
def test_update(sql_gateway: SyncSQLGateway, if_unmodified_since: datetime | None):
    records = [{"id": 2, "value": "foo"}]
    sql_gateway.provider.execute.return_value = records
    assert sql_gateway.update({"id": 2, "name": "foo"}, if_unmodified_since) == {
        "id": 2,
        "name": "foo",
    }

    # query builder was called with mapped record
    sql_gateway.builder.update.assert_called_once_with(
        2, {"id": 2, "value": "foo"}, if_unmodified_since
    )

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.update.return_value
    )


@pytest.mark.parametrize("item", [{"value": "foo"}, {"id": None, "value": "foo"}])
def test_update_no_id(sql_gateway: SyncSQLGateway, item: Json):
    with pytest.raises(DoesNotExist):
        sql_gateway.update(item)

    # query builder and provider were not called
    assert not sql_gateway.builder.update.called
    assert not sql_gateway.provider.execute.called


def test_update_not_found(sql_gateway: SyncSQLGateway):
    sql_gateway.provider.execute.return_value = []
    with pytest.raises(DoesNotExist):
        assert sql_gateway.update({"id": 2, "name": "foo"})

    # query builder and provider were called (call args tested earlier)
    assert sql_gateway.builder.update.called
    assert sql_gateway.provider.execute.called


@mock.patch.object(SyncSQLGateway, "exists")
def test_update_if_unmodified_since_not_found(exists, sql_gateway: SyncSQLGateway):
    exists.return_value = False
    sql_gateway.provider.execute.return_value = []
    with pytest.raises(DoesNotExist):
        assert sql_gateway.update({"id": 2, "name": "foo"}, if_unmodified_since=DT)

    # query builder and provider were called (call args tested earlier)
    assert sql_gateway.builder.update.called
    assert sql_gateway.provider.execute.called

    # existence of record was checked to raise the correct error
    exists.assert_called_once_with([Filter.for_id(2)])


@mock.patch.object(SyncSQLGateway, "exists")
def test_update_if_unmodified_since_conflict(exists, sql_gateway: SyncSQLGateway):
    exists.return_value = True
    sql_gateway.provider.execute.return_value = []
    with pytest.raises(Conflict):
        assert sql_gateway.update({"id": 2, "name": "foo"}, if_unmodified_since=DT)

    # query builder and provider were called (call args tested earlier)
    assert sql_gateway.builder.update.called
    assert sql_gateway.provider.execute.called

    # existence of record was checked to raise the correct error
    exists.assert_called_once_with([Filter.for_id(2)])


def test_upsert(sql_gateway: SyncSQLGateway):
    records = [{"id": 2, "value": "foo"}]
    sql_gateway.provider.execute.return_value = records
    assert sql_gateway.upsert({"id": 2, "name": "foo"}) == {"id": 2, "name": "foo"}

    # query builder was called with mapped record
    sql_gateway.builder.upsert.assert_called_once_with({"id": 2, "value": "foo"})

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.upsert.return_value
    )


@pytest.mark.parametrize("item", [{"value": "foo"}, {"id": None, "value": "foo"}])
@mock.patch.object(SyncSQLGateway, "add")
def test_upsert_no_id(add_m, sql_gateway: SyncSQLGateway, item: Json):
    # upsert reduces to add if no id is given
    assert sql_gateway.upsert(item) is add_m.return_value
    add_m.assert_called_once_with(item)


@pytest.mark.parametrize("records,expected", [([{"id": 2}], True), ([], False)])
def test_remove(sql_gateway: SyncSQLGateway, records, expected):
    sql_gateway.provider.execute.return_value = records
    assert sql_gateway.remove(2) is expected

    # query builder was called with id
    sql_gateway.builder.delete.assert_called_once_with(2)

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.delete.return_value
    )


def test_filter(sql_gateway: SyncSQLGateway):
    sql_gateway.provider.execute.return_value = [
        {"id": 2, "value": "foo"},
        {"id": 3, "value": "bar"},
    ]
    args = ("a", "b")
    assert sql_gateway.filter(*args) == [
        {"id": 2, "name": "foo"},
        {"id": 3, "name": "bar"},
    ]

    # query builder was called with filters and params
    sql_gateway.builder.select.assert_called_once_with(*args)

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.select.return_value
    )


def test_count(sql_gateway: SyncSQLGateway):
    sql_gateway.provider.execute.return_value = [{"count": 15}]
    assert sql_gateway.count("a") == 15

    # query builder was called with filters
    sql_gateway.builder.count.assert_called_once_with("a")

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.count.return_value
    )


@pytest.mark.parametrize("exists", [False, True])
def test_exists(sql_gateway: SyncSQLGateway, exists: bool):
    sql_gateway.provider.execute.return_value = [{"exists": True}] if exists else []
    assert sql_gateway.exists("a") is exists

    # query builder was called with filters
    sql_gateway.builder.exists.assert_called_once_with("a")

    # provider was called with query
    sql_gateway.provider.execute.assert_called_once_with(
        sql_gateway.builder.exists.return_value
    )
