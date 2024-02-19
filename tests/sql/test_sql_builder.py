from datetime import datetime
from datetime import timezone

import pytest
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

from clean_python import Filter
from clean_python import Json
from clean_python import PageOptions
from clean_python.sql import SQLBuilder
from clean_python.sql.testing import assert_query_equal

writer = Table(
    "writer",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


ALL_FIELDS = "writer.id, writer.value, writer.updated_at"


@pytest.fixture
def sql_builder() -> SQLBuilder:
    return SQLBuilder(writer, multitenant=False)


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
def test_select(sql_builder: SQLBuilder, filters: list[Filter], sql: str):
    query = sql_builder.select(filters, None)
    assert_query_equal(query, f"SELECT {ALL_FIELDS} FROM writer{sql}")


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
def test_select_with_pagination(
    sql_builder: SQLBuilder, page_options: PageOptions, sql: str
):
    query = sql_builder.select([], page_options)
    assert_query_equal(query, f"SELECT {ALL_FIELDS} FROM writer{sql}")


def test_select_filter_and_pagination(sql_builder: SQLBuilder):
    query = sql_builder.select(
        [Filter(field="value", values=["foo"])],
        PageOptions(limit=5, order_by="id"),
    )
    assert_query_equal(
        query,
        (
            f"SELECT {ALL_FIELDS} FROM writer "
            f"WHERE writer.value = 'foo' "
            f"ORDER BY writer.id ASC LIMIT 5 OFFSET 0"
        ),
    )


def test_select_for_update(sql_builder: SQLBuilder):
    query = sql_builder.select([Filter.for_id(2)], for_update=True)
    assert_query_equal(
        query,
        f"SELECT {ALL_FIELDS} FROM writer WHERE writer.id = 2 FOR UPDATE",
    )


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
def test_count(sql_builder: SQLBuilder, filters: list[Filter], sql: str):
    query = sql_builder.count(filters)
    assert_query_equal(query, f"SELECT count(*) AS count FROM writer{sql}")


@pytest.mark.parametrize(
    "record,sql",
    [
        ({}, "DEFAULT VALUES"),
        ({"value": "foo"}, "(value) VALUES ('foo')"),
        ({"id": None, "value": "foo"}, "(value) VALUES ('foo')"),
        ({"id": 2, "value": "foo"}, "(id, value) VALUES (2, 'foo')"),
        ({"value": "foo", "nonexisting": 2}, "(value) VALUES ('foo')"),
    ],
)
async def test_insert(sql_builder: SQLBuilder, record: Json, sql: str):
    query = sql_builder.insert(record)
    assert_query_equal(query, (f"INSERT INTO writer {sql} RETURNING {ALL_FIELDS}"))


@pytest.mark.parametrize(
    "record,if_unmodified_since,sql",
    [
        (
            {"id": 2, "value": "foo"},
            None,
            "SET id=2, value='foo' WHERE writer.id = 2",
        ),
        ({"id": 2, "other": "foo"}, None, "SET id=2 WHERE writer.id = 2"),
        (
            {"id": 2, "value": "foo"},
            datetime(2010, 1, 1, tzinfo=timezone.utc),
            (
                "SET id=2, value='foo' WHERE writer.id = 2 "
                "AND writer.updated_at = '2010-01-01 00:00:00+00:00'"
            ),
        ),
    ],
)
async def test_update(
    sql_builder: SQLBuilder,
    record: Json,
    if_unmodified_since: datetime | None,
    sql: str,
):
    query = sql_builder.update(record["id"], record, if_unmodified_since)
    assert_query_equal(query, (f"UPDATE writer {sql} RETURNING {ALL_FIELDS}"))


async def test_delete(sql_builder: SQLBuilder):
    query = sql_builder.delete(2)
    assert_query_equal(
        query,
        "DELETE FROM writer WHERE writer.id = 2 RETURNING writer.id",
    )


async def test_upsert(sql_builder: SQLBuilder):
    query = sql_builder.upsert({"id": 2, "value": "foo"})
    assert_query_equal(
        query,
        (
            f"INSERT INTO writer (id, value) VALUES (2, 'foo') "
            f"ON CONFLICT (id) DO UPDATE SET "
            f"id = %(param_1)s, value = %(param_2)s "
            f"RETURNING {ALL_FIELDS}"
        ),
    )


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
async def test_exists(sql_builder: SQLBuilder, filters: list[Filter], sql: str):
    query = sql_builder.exists(filters)
    assert_query_equal(
        query,
        f"SELECT true AS exists FROM writer{sql} LIMIT 1",
    )
