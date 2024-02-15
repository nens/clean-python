import pytest
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

from clean_python import Filter
from clean_python import PageOptions
from clean_python.sql import SyncSQLGateway
from clean_python.sql.testing import assert_query_equal
from clean_python.sql.testing import FakeSyncSQLDatabase

writer = Table(
    "writer",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


ALL_FIELDS = "writer.id, writer.value, writer.updated_at"


class TstSQLGateway(SyncSQLGateway):
    table = writer


@pytest.fixture
def sql_gateway():
    return TstSQLGateway(FakeSyncSQLDatabase())


@pytest.mark.parametrize(
    "filters,sql",
    [
        ([], ""),
        ([Filter(field="value", values=[])], " WHERE false"),
        ([Filter(field="value", values=["foo"])], " WHERE writer.value = 'foo'"),
        (
            [Filter(field="value", values=["foo", "bar"])],
            " WHERE writer.value IN ('foo', 'bar')",
        ),
        ([Filter(field="nonexisting", values=["foo"])], " WHERE false"),
        (
            [Filter(field="id", values=[1]), Filter(field="value", values=["foo"])],
            " WHERE writer.id = 1 AND writer.value = 'foo'",
        ),
    ],
)
def test_filter(sql_gateway, filters, sql):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert sql_gateway.filter(filters) == [{"id": 2, "value": "foo"}]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT {ALL_FIELDS} FROM writer{sql}",
    )


@pytest.mark.parametrize(
    "page_options,sql",
    [
        (None, ""),
        (
            PageOptions(limit=5, order_by="id"),
            " ORDER BY writer.id ASC LIMIT 5 OFFSET 0",
        ),
        (
            PageOptions(limit=5, offset=2, order_by="id", ascending=False),
            " ORDER BY writer.id DESC LIMIT 5 OFFSET 2",
        ),
    ],
)
def test_filter_with_pagination(sql_gateway, page_options, sql):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert sql_gateway.filter([], params=page_options) == [{"id": 2, "value": "foo"}]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT {ALL_FIELDS} FROM writer{sql}",
    )


def test_filter_with_pagination_and_filter(sql_gateway):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert sql_gateway.filter(
        [Filter(field="value", values=["foo"])],
        params=PageOptions(limit=5, order_by="id"),
    ) == [{"id": 2, "value": "foo"}]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"SELECT {ALL_FIELDS} FROM writer "
            f"WHERE writer.value = 'foo' "
            f"ORDER BY writer.id ASC LIMIT 5 OFFSET 0"
        ),
    )
