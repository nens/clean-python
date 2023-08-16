from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import PageOptions
from clean_python.sql import SQLGateway
from clean_python.sql.testing import assert_query_equal
from clean_python.sql.testing import FakeSQLDatabase

writer = Table(
    "writer",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


book = Table(
    "book",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", Text, nullable=False),
    Column(
        "writer_id",
        Integer,
        ForeignKey("writer.id", ondelete="CASCADE", name="book_writer_id_fkey"),
        nullable=False,
    ),
)


ALL_FIELDS = "writer.id, writer.value, writer.updated_at"
BOOK_FIELDS = "book.id, book.title, book.writer_id"


class TstSQLGateway(SQLGateway, table=writer):
    pass


class TstRelatedSQLGateway(SQLGateway, table=book):
    pass


@pytest.fixture
def sql_gateway():
    return TstSQLGateway(FakeSQLDatabase())


@pytest.fixture
def related_sql_gateway():
    return TstRelatedSQLGateway(FakeSQLDatabase())


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
async def test_filter(sql_gateway, filters, sql):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert await sql_gateway.filter(filters) == [{"id": 2, "value": "foo"}]
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
async def test_filter_with_pagination(sql_gateway, page_options, sql):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert await sql_gateway.filter([], params=page_options) == [
        {"id": 2, "value": "foo"}
    ]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT {ALL_FIELDS} FROM writer{sql}",
    )


async def test_filter_with_pagination_and_filter(sql_gateway):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert await sql_gateway.filter(
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
async def test_count(sql_gateway, filters, sql):
    sql_gateway.provider.result.return_value = [{"count": 4}]
    assert await sql_gateway.count(filters) == 4
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT count(*) AS count FROM writer{sql}",
    )


@mock.patch.object(SQLGateway, "filter")
async def test_get(filter_m, sql_gateway):
    filter_m.return_value = [{"id": 2, "value": "foo"}]
    assert await sql_gateway.get(2) == filter_m.return_value[0]
    assert len(sql_gateway.provider.queries) == 0
    filter_m.assert_awaited_once_with([Filter(field="id", values=[2])], params=None)


@mock.patch.object(SQLGateway, "filter")
async def test_get_does_not_exist(filter_m, sql_gateway):
    filter_m.return_value = []
    assert await sql_gateway.get(2) is None
    assert len(sql_gateway.provider.queries) == 0
    filter_m.assert_awaited_once_with([Filter(field="id", values=[2])], params=None)


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
async def test_add(sql_gateway, record, sql):
    records = [{"id": 2, "value": "foo"}]
    sql_gateway.provider.result.return_value = records
    assert await sql_gateway.add(record) == records[0]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (f"INSERT INTO writer {sql} RETURNING {ALL_FIELDS}"),
    )


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
async def test_update(sql_gateway, record, if_unmodified_since, sql):
    records = [{"id": 2, "value": "foo"}]
    sql_gateway.provider.result.return_value = records
    assert await sql_gateway.update(record, if_unmodified_since) == records[0]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (f"UPDATE writer {sql} RETURNING {ALL_FIELDS}"),
    )


async def test_update_does_not_exist(sql_gateway):
    sql_gateway.provider.result.return_value = []
    with pytest.raises(DoesNotExist):
        await sql_gateway.update({"id": 2})
    assert len(sql_gateway.provider.queries) == 1


@mock.patch.object(SQLGateway, "get")
async def test_update_if_unmodified_since_does_not_exist(get_m, sql_gateway):
    get_m.return_value = None
    sql_gateway.provider.result.return_value = []
    with pytest.raises(DoesNotExist):
        await sql_gateway.update(
            {"id": 2}, if_unmodified_since=datetime(2010, 1, 1, tzinfo=timezone.utc)
        )
    assert len(sql_gateway.provider.queries) == 1
    get_m.assert_awaited_once_with(2)


@mock.patch.object(SQLGateway, "get")
async def test_update_if_unmodified_since_conflict(get_m, sql_gateway):
    get_m.return_value = {"id": 2, "value": "foo"}
    sql_gateway.provider.result.return_value = []
    with pytest.raises(Conflict):
        await sql_gateway.update(
            {"id": 2}, if_unmodified_since=datetime(2010, 1, 1, tzinfo=timezone.utc)
        )
    assert len(sql_gateway.provider.queries) == 1
    get_m.assert_awaited_once_with(2)


async def test_remove(sql_gateway):
    sql_gateway.provider.result.return_value = [{"id": 2}]
    assert (await sql_gateway.remove(2)) is True
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        ("DELETE FROM writer WHERE writer.id = 2 RETURNING writer.id"),
    )


async def test_remove_does_not_exist(sql_gateway):
    sql_gateway.provider.result.return_value = []
    assert (await sql_gateway.remove(2)) is False
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        ("DELETE FROM writer WHERE writer.id = 2 RETURNING writer.id"),
    )


async def test_upsert(sql_gateway):
    record = {"id": 2, "value": "foo"}
    sql_gateway.provider.result.return_value = [record]
    assert await sql_gateway.upsert(record) == record
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"INSERT INTO writer (id, value) VALUES (2, 'foo') "
            f"ON CONFLICT (id) DO UPDATE SET "
            f"id = %(param_1)s, value = %(param_2)s "
            f"RETURNING {ALL_FIELDS}"
        ),
    )


@mock.patch.object(SQLGateway, "add")
async def test_upsert_no_id(add_m, sql_gateway):
    add_m.return_value = {"id": 5, "value": "foo"}
    assert await sql_gateway.upsert({"value": "foo"}) == add_m.return_value

    add_m.assert_awaited_once_with({"value": "foo"})
    assert len(sql_gateway.provider.queries) == 0


async def test_get_related_one_to_many(related_sql_gateway: SQLGateway):
    writers = [{"id": 2}, {"id": 3}]
    books = [
        {"id": 3, "title": "x", "writer_id": 2},
        {"id": 4, "title": "y", "writer_id": 2},
    ]
    related_sql_gateway.provider.result.return_value = books
    await related_sql_gateway._get_related_one_to_many(
        items=writers,
        field_name="books",
        fk_name="writer_id",
    )

    assert writers == [{"id": 2, "books": books}, {"id": 3, "books": []}]
    assert len(related_sql_gateway.provider.queries) == 1
    assert_query_equal(
        related_sql_gateway.provider.queries[0][0],
        (
            "SELECT book.id, book.title, book.writer_id FROM book WHERE book.writer_id IN (2, 3)"
        ),
    )


@pytest.mark.parametrize(
    "books,current_books,expected_queries,query_results",
    [
        # no change
        (
            [{"id": 3, "title": "x", "writer_id": 2}],
            [{"id": 3, "title": "x", "writer_id": 2}],
            [],
            [],
        ),
        # added a book (without an id)
        (
            [{"title": "x", "writer_id": 2}],
            [],
            [
                f"INSERT INTO book (title, writer_id) VALUES ('x', 2) RETURNING {BOOK_FIELDS}"
            ],
            [[{"id": 3, "title": "x", "writer_id": 2}]],
        ),
        # added a book (with an id)
        (
            [{"id": 3, "title": "x", "writer_id": 2}],
            [],
            [
                f"INSERT INTO book (id, title, writer_id) VALUES (3, 'x', 2) RETURNING {BOOK_FIELDS}"
            ],
            [[{"id": 3, "title": "x", "writer_id": 2}]],
        ),
        # updated a book
        (
            [{"id": 3, "title": "x", "writer_id": 2}],
            [{"id": 3, "title": "a", "writer_id": 2}],
            [
                f"UPDATE book SET id=3, title='x', writer_id=2 WHERE book.id = 3 RETURNING {BOOK_FIELDS}"
            ],
            [[{"id": 3, "title": "x", "writer_id": 2}]],
        ),
        # replaced a book with a new one
        (
            [{"title": "x", "writer_id": 2}],
            [{"id": 15, "title": "a", "writer_id": 2}],
            [
                f"INSERT INTO book (title, writer_id) VALUES ('x', 2) RETURNING {BOOK_FIELDS}",
                "DELETE FROM book WHERE book.id = 15 RETURNING book.id",
            ],
            [[{"id": 3, "title": "x", "writer_id": 2}], [{"id": 15}]],
        ),
    ],
)
async def test_set_related_one_to_many(
    related_sql_gateway: SQLGateway,
    books,
    current_books,
    expected_queries,
    query_results,
):
    writer = {"id": 2, "books": books}
    related_sql_gateway.provider.result.side_effect = [current_books] + query_results
    result = writer.copy()
    await related_sql_gateway._set_related_one_to_many(
        item=writer,
        result=result,
        field_name="books",
        fk_name="writer_id",
    )

    assert result == {
        "id": 2,
        "books": [{"id": 3, "title": "x", "writer_id": 2}],
    }
    assert len(related_sql_gateway.provider.queries) == len(expected_queries) + 1
    assert_query_equal(
        related_sql_gateway.provider.queries[0][0],
        f"SELECT {BOOK_FIELDS} FROM book WHERE book.writer_id = 2",
    )
    for (actual_query,), expected_query in zip(
        related_sql_gateway.provider.queries[1:], expected_queries
    ):
        assert_query_equal(actual_query, expected_query)


async def test_update_transactional(sql_gateway):
    existing = {"id": 2, "value": "foo"}
    expected = {"id": 2, "value": "bar"}
    sql_gateway.provider.result.side_effect = ([existing], [expected])
    actual = await sql_gateway.update_transactional(
        2, lambda x: {"id": x["id"], "value": "bar"}
    )
    assert actual == expected

    (queries,) = sql_gateway.provider.queries
    assert len(queries) == 2
    assert_query_equal(
        queries[0],
        f"SELECT {ALL_FIELDS} FROM writer WHERE writer.id = 2 FOR UPDATE",
    )
    assert_query_equal(
        queries[1],
        (
            f"UPDATE writer SET id=2, value='bar' WHERE writer.id = 2 RETURNING {ALL_FIELDS}"
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
async def test_exists(sql_gateway, filters, sql):
    sql_gateway.provider.result.return_value = [{"exists": True}]
    assert await sql_gateway.exists(filters) is True
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT true AS exists FROM writer{sql} LIMIT 1",
    )
