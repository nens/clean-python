# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans
import asyncio
from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python.sql import SQLDatabase
from clean_python.sql import SQLGateway

test_model = Table(
    "test_model",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("t", Text, nullable=False),
    Column("f", Float, nullable=False),
    Column("b", Boolean, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("n", Float, nullable=True),
)


### SQLProvider integration tests
count_query = text("SELECT COUNT(*) FROM test_model")
insert_query = text(
    "INSERT INTO test_model (t, f, b, updated_at) "
    "VALUES ('foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
    "RETURNING id"
)
update_query = text("UPDATE test_model SET t='bar' WHERE id=:id RETURNING t")


@pytest.fixture(scope="session")
def dburl(postgres_url):
    return f"postgresql+asyncpg://{postgres_url}"


@pytest.fixture(scope="session")
def dbname():
    return "cleanpython_test"


@pytest.fixture(scope="session")
async def database(dburl, dbname):
    root_provider = SQLDatabase(f"{dburl}/")
    await root_provider.drop_database(dbname)
    await root_provider.create_database(dbname)
    provider = SQLDatabase(f"{dburl}/{dbname}")
    async with provider.engine.begin() as conn:
        await conn.run_sync(test_model.metadata.drop_all)
        await conn.run_sync(test_model.metadata.create_all)
    yield SQLDatabase(f"{dburl}/{dbname}")


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


@pytest.fixture
async def database_no_pool_no_cache(dburl, dbname):
    db = SQLDatabase(
        f"{dburl}/{dbname}?prepared_statement_cache_size=0", poolclass=NullPool
    )
    yield db
    await db.truncate_tables(["test_model"])


async def test_handle_serialization_error(
    database_no_pool_no_cache: SQLDatabase, record_id: int
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
        async with database_no_pool_no_cache.transaction() as trans:
            res = await trans.execute(update_query, bind_params={"id": record_id})
            await asyncio.sleep(sleep_after)
        return res

    res1, res2 = await asyncio.gather(
        update(sleep_after=0.02), update(sleep_before=0.01), return_exceptions=True
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


### SQLGateway integration tests


class TstSQLGateway(SQLGateway, table=test_model):
    pass


@pytest.fixture
async def test_transaction(database):
    async with database.testing_transaction() as test_transaction:
        yield test_transaction


@pytest.fixture
def sql_gateway(test_transaction):
    return TstSQLGateway(test_transaction)


@pytest.fixture
def obj():
    return {
        "t": "foo",
        "f": 1.23,
        "b": True,
        "updated_at": datetime(2016, 6, 23, 2, 10, 25, tzinfo=timezone.utc),
        "n": None,
    }


@pytest.fixture
async def obj_in_db(test_transaction, obj):
    res = await test_transaction.execute(
        text(
            "INSERT INTO test_model (t, f, b, updated_at) "
            "VALUES ('foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
            "RETURNING id"
        )
    )
    return {"id": res[0]["id"], **obj}


async def test_get(sql_gateway, obj_in_db):
    actual = await sql_gateway.get(obj_in_db["id"])

    assert isinstance(actual, dict)
    assert actual == obj_in_db
    assert actual is not obj_in_db


async def test_get_not_found(sql_gateway, obj_in_db):
    assert await sql_gateway.get(obj_in_db["id"] + 1) is None


async def test_add(sql_gateway, test_transaction, obj):
    created = await sql_gateway.add(obj)

    id = created.pop("id")
    assert isinstance(id, int)
    assert created is not obj
    assert created == obj

    res = await test_transaction.execute(
        text(f"SELECT * FROM test_model WHERE id = {id}")
    )
    assert res[0]["t"] == obj["t"]


async def test_add_id_exists(sql_gateway, obj_in_db):
    with pytest.raises(
        AlreadyExists, match=f"record with id={obj_in_db['id']} already exists"
    ):
        await sql_gateway.add(obj_in_db)


@pytest.mark.parametrize("id", [10, None, "delete"])
async def test_add_integrity_error(sql_gateway, obj, id):
    obj.pop("t")  # will cause the IntegrityError
    if id != "delete":
        obj["id"] = id
    with pytest.raises(IntegrityError):
        await sql_gateway.add(obj)


async def test_add_unkown_column(sql_gateway, obj):
    created = await sql_gateway.add({"unknown": "foo", **obj})

    created.pop("id")
    assert created == obj


async def test_update(sql_gateway, test_transaction, obj_in_db):
    obj_in_db["t"] = "bar"

    updated = await sql_gateway.update(obj_in_db)

    assert updated is not obj_in_db
    assert updated == obj_in_db

    res = await test_transaction.execute(
        text(f"SELECT * FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["t"] == "bar"


async def test_update_not_found(sql_gateway, obj):
    obj["id"] = 42

    with pytest.raises(DoesNotExist):
        await sql_gateway.update(obj)


async def test_update_unkown_column(sql_gateway, obj_in_db):
    obj_in_db["t"] = "bar"
    updated = await sql_gateway.update({"unknown": "foo", **obj_in_db})

    assert updated == obj_in_db


async def test_upsert_does_add(sql_gateway, test_transaction, obj):
    obj["id"] = 42
    created = await sql_gateway.upsert(obj)

    assert created is not obj
    assert created == obj

    res = await test_transaction.execute(text("SELECT * FROM test_model WHERE id = 42"))
    assert res[0]["t"] == obj["t"]


async def test_upsert_does_update(sql_gateway, test_transaction, obj_in_db):
    obj_in_db["t"] = "bar"
    updated = await sql_gateway.upsert(obj_in_db)

    assert updated is not obj_in_db
    assert updated == obj_in_db

    res = await test_transaction.execute(
        text(f"SELECT * FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["t"] == "bar"


async def test_upsert_no_id(sql_gateway, test_transaction, obj):
    with mock.patch.object(sql_gateway, "add", new_callable=mock.AsyncMock) as add_m:
        created = await sql_gateway.upsert(obj)
        add_m.assert_awaited_with(obj)
        assert created == add_m.return_value


async def test_remove(sql_gateway, test_transaction, obj_in_db):
    assert await sql_gateway.remove(obj_in_db["id"])

    res = await test_transaction.execute(
        text(f"SELECT COUNT(*) FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["count"] == 0


async def test_remove_not_found(sql_gateway):
    assert not await sql_gateway.remove(42)


async def test_update_if_unmodified_since(sql_gateway, obj_in_db):
    obj_in_db["t"] = "bar"

    updated = await sql_gateway.update(
        obj_in_db, if_unmodified_since=obj_in_db["updated_at"]
    )

    assert updated == obj_in_db


@pytest.mark.parametrize(
    "if_unmodified_since", [datetime.now(timezone.utc), datetime(2010, 1, 1)]
)
async def test_update_if_unmodified_since_not_ok(
    sql_gateway, obj_in_db, if_unmodified_since
):
    obj_in_db["t"] = "bar"

    with pytest.raises(Conflict):
        await sql_gateway.update(obj_in_db, if_unmodified_since=if_unmodified_since)


@pytest.mark.parametrize(
    "filters,match",
    [
        ([], True),
        ([Filter(field="t", values=["foo"])], True),
        ([Filter(field="t", values=["bar"])], False),
        ([Filter(field="t", values=["foo"]), Filter(field="f", values=[1.23])], True),
        ([Filter(field="t", values=["foo"]), Filter(field="f", values=[1.24])], False),
        ([Filter(field="nonexisting", values=["foo"])], False),
        ([Filter(field="t", values=[])], False),
        ([Filter(field="t", values=["foo", "bar"])], True),
    ],
)
async def test_filter(filters, match, sql_gateway, obj_in_db):
    actual = await sql_gateway.filter(filters)

    assert actual == ([obj_in_db] if match else [])


@pytest.fixture
async def obj2_in_db(test_transaction, obj):
    res = await test_transaction.execute(
        text(
            "INSERT INTO test_model (t, f, b, updated_at) "
            "VALUES ('bar', 1.24, TRUE, '2018-06-22 19:10:25-07') "
            "RETURNING id"
        )
    )
    return {"id": res[0]["id"], **obj}


@pytest.mark.parametrize(
    "filters,expected",
    [
        ([], 2),
        ([Filter(field="t", values=["foo"])], 1),
        ([Filter(field="t", values=["bar"])], 1),
        ([Filter(field="t", values=["baz"])], 0),
    ],
)
async def test_count(filters, expected, sql_gateway, obj_in_db, obj2_in_db):
    actual = await sql_gateway.count(filters)
    assert actual == expected


@pytest.mark.parametrize(
    "filters,expected",
    [
        ([], True),
        ([Filter(field="t", values=["foo"])], True),
        ([Filter(field="t", values=["bar"])], True),
        ([Filter(field="t", values=["baz"])], False),
    ],
)
async def test_exists(filters, expected, sql_gateway, obj_in_db, obj2_in_db):
    actual = await sql_gateway.exists(filters)
    assert actual == expected


async def test_truncate(database: SQLDatabase, obj):
    gateway = TstSQLGateway(database)
    await gateway.add(obj)
    assert await gateway.exists([])
    await database.truncate_tables(["test_model"])
    assert not await gateway.exists([])
