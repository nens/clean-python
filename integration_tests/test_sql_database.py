# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans
import asyncio

import pytest
from sql_model import count_query
from sql_model import insert_query
from sql_model import update_query
from sqlalchemy.sql import text

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python.sql import AsyncpgSQLDatabase
from clean_python.sql import SQLAlchemyAsyncSQLDatabase
from clean_python.sql import SQLDatabase


@pytest.fixture(
    scope="session", params=[SQLAlchemyAsyncSQLDatabase, AsyncpgSQLDatabase]
)
async def database(request, postgres_db_url):
    # pool_size=2 for Conflict test
    db = request.param(postgres_db_url, pool_size=2)
    yield db
    await db.dispose()


@pytest.fixture
async def database_with_cleanup(database: SQLDatabase):
    await database.truncate_tables(["test_model"])
    yield database
    await database.truncate_tables(["test_model"])


@pytest.fixture
async def transaction_with_cleanup(database_with_cleanup):
    async with database_with_cleanup.transaction() as trans:
        yield trans


@pytest.fixture
async def record_id(database_with_cleanup: SQLDatabase) -> int:
    record = await database_with_cleanup.execute(insert_query)
    return record[0]["id"]


async def test_execute(database_with_cleanup):
    db = database_with_cleanup
    await db.execute(insert_query)
    assert await db.execute(count_query) == [{"count": 1}]


async def test_transaction_commits(database_with_cleanup):
    db = database_with_cleanup

    async with db.transaction() as trans:
        await trans.execute(insert_query)

    assert await db.execute(count_query) == [{"count": 1}]


async def test_transaction_err(database_with_cleanup):
    db = database_with_cleanup
    await db.execute(insert_query)

    with pytest.raises(RuntimeError):
        async with db.transaction() as trans:
            await trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert await db.execute(count_query) == [{"count": 1}]


async def test_nested_transaction_commits(transaction_with_cleanup):
    db = transaction_with_cleanup

    async with db.transaction() as trans:
        await trans.execute(insert_query)

    assert await db.execute(count_query) == [{"count": 1}]


async def test_nested_transaction_err(transaction_with_cleanup):
    db = transaction_with_cleanup
    await db.execute(insert_query)

    with pytest.raises(RuntimeError):
        async with db.transaction() as trans:
            await trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert await db.execute(count_query) == [{"count": 1}]


async def test_testing_transaction_rollback(database_with_cleanup):
    async with database_with_cleanup.testing_transaction() as trans:
        await trans.execute(insert_query)

    assert await database_with_cleanup.execute(count_query) == [{"count": 0}]


async def test_handle_serialization_error(
    database_with_cleanup: SQLDatabase, record_id: int
):
    """Typical 'lost update' situation will result in a Conflict error

    1> BEGIN
    1> UPDATE ... WHERE id=1
    2> BEGIN
    2> UPDATE ... WHERE id=1   # transaction 2 will wait until transaction 1 finishes
    1> COMMIT
    2> will raise SerializationError
    """

    async def update(sleep_before=0.0, sleep_after=0.0):
        await asyncio.sleep(sleep_before)
        async with database_with_cleanup.transaction() as trans:
            res = await trans.execute(update_query, bind_params={"id": record_id})
            await asyncio.sleep(sleep_after)
        return res

    res1, res2 = await asyncio.gather(
        update(sleep_after=0.1), update(sleep_before=0.05), return_exceptions=True
    )
    assert res1 == [{"t": "bar"}]
    assert isinstance(res2, Conflict)
    assert str(res2) == "could not execute query due to concurrent update"


async def test_handle_integrity_error(
    database_with_cleanup: SQLDatabase, record_id: int
):
    """Insert a record with an id that already exists"""
    insert_query_with_id = text(
        "INSERT INTO test_model (id, t, f, b, updated_at) "
        "VALUES (:id, 'foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
        "RETURNING id"
    )

    with pytest.raises(
        AlreadyExists, match=f"record with id={record_id} already exists"
    ):
        await database_with_cleanup.execute(
            insert_query_with_id, bind_params={"id": record_id}
        )
