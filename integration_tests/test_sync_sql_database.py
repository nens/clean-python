# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans
import asyncio
from datetime import datetime
from datetime import timezone
from unittest import mock

import pytest
from asyncpg.exceptions import NotNullViolationError
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy.sql import text

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter

# from clean_python.sql.asyncpg_sql_database import AsyncpgSQLDatabase as SQLDatabase
from clean_python.sql import SQLGateway
from clean_python.sql.sqlalchemy_sync_sql_database import (
    SQLAlchemySyncSQLDatabase as SQLDatabase,
)

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
def dbname():
    return "cleanpython_test"


@pytest.fixture(scope="session")
def database(postgres_url, dbname):
    root_provider = SQLDatabase(f"{postgres_url}/")
    root_provider.drop_database(dbname)
    root_provider.create_database(dbname)
    root_provider.dispose()
    provider = SQLDatabase(f"{postgres_url}/{dbname}", pool_size=2)
    test_model.metadata.drop_all(provider.engine)
    test_model.metadata.create_all(provider.engine)
    yield provider
    provider.dispose()


@pytest.fixture
def database_with_cleanup(database: SQLDatabase):
    database.truncate_tables(["test_model"])
    yield database
    database.truncate_tables(["test_model"])


@pytest.fixture
def transaction_with_cleanup(database_with_cleanup):
    with database_with_cleanup.transaction() as trans:
        yield trans


@pytest.fixture
def record_id(database_with_cleanup: SQLDatabase) -> int:
    record = database_with_cleanup.execute(insert_query)
    return record[0]["id"]


def test_execute(database_with_cleanup):
    db = database_with_cleanup
    db.execute(insert_query)
    assert db.execute(count_query) == [{"count": 1}]


def test_transaction_commits(database_with_cleanup):
    db = database_with_cleanup

    with db.transaction() as trans:
        trans.execute(insert_query)

    assert db.execute(count_query) == [{"count": 1}]


def test_transaction_err(database_with_cleanup):
    db = database_with_cleanup
    db.execute(insert_query)

    with pytest.raises(RuntimeError):
        with db.transaction() as trans:
            trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert db.execute(count_query) == [{"count": 1}]


def test_nested_transaction_commits(transaction_with_cleanup):
    db = transaction_with_cleanup

    with db.transaction() as trans:
        trans.execute(insert_query)

    assert db.execute(count_query) == [{"count": 1}]


def test_nested_transaction_err(transaction_with_cleanup):
    db = transaction_with_cleanup
    db.execute(insert_query)

    with pytest.raises(RuntimeError):
        with db.transaction() as trans:
            trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert db.execute(count_query) == [{"count": 1}]


def test_testing_transaction_rollback(database_with_cleanup):
    with database_with_cleanup.testing_transaction() as trans:
        trans.execute(insert_query)

    assert database_with_cleanup.execute(count_query) == [{"count": 0}]


def test_handle_serialization_error(database_with_cleanup: SQLDatabase, record_id: int):
    """Typical 'lost update' situation will result in a Conflict error

    1> BEGIN
    1> UPDATE ... WHERE id=1
    2> BEGIN
    2> UPDATE ... WHERE id=1   # transaction 2 will wait until transaction 1 finishes
    1> COMMIT
    2> will raise SerializationError
    """

    def update(sleep_before=0.0, sleep_after=0.0):
        asyncio.sleep(sleep_before)
        with database_with_cleanup.transaction() as trans:
            res = trans.execute(update_query, bind_params={"id": record_id})
            asyncio.sleep(sleep_after)
        return res

    res1, res2 = asyncio.gather(
        update(sleep_after=0.1), update(sleep_before=0.05), return_exceptions=True
    )
    assert res1 == [{"t": "bar"}]
    assert isinstance(res2, Conflict)
    assert str(res2) == "could not execute query due to concurrent update"


def test_handle_integrity_error(database_with_cleanup: SQLDatabase, record_id: int):
    """Insert a record with an id that already exists"""
    insert_query_with_id = text(
        "INSERT INTO test_model (id, t, f, b, updated_at) "
        "VALUES (:id, 'foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
        "RETURNING id"
    )

    with pytest.raises(
        AlreadyExists, match=f"record with id={record_id} already exists"
    ):
        database_with_cleanup.execute(
            insert_query_with_id, bind_params={"id": record_id}
        )


### SQLGateway integration tests


class TstSQLGateway(SQLGateway, table=test_model):
    pass


@pytest.fixture
def test_transaction(database):
    with database.testing_transaction() as test_transaction:
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
def obj_in_db(test_transaction, obj):
    res = test_transaction.execute(
        text(
            "INSERT INTO test_model (t, f, b, updated_at) "
            "VALUES ('foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
            "RETURNING id"
        )
    )
    return {"id": res[0]["id"], **obj}


def test_get(sql_gateway, obj_in_db):
    actual = sql_gateway.get(obj_in_db["id"])

    assert isinstance(actual, dict)
    assert actual == obj_in_db
    assert actual is not obj_in_db


def test_get_not_found(sql_gateway, obj_in_db):
    assert sql_gateway.get(obj_in_db["id"] + 1) is None


def test_add(sql_gateway, test_transaction, obj):
    created = sql_gateway.add(obj)

    id = created.pop("id")
    assert isinstance(id, int)
    assert created is not obj
    assert created == obj

    res = test_transaction.execute(text(f"SELECT * FROM test_model WHERE id = {id}"))
    assert res[0]["t"] == obj["t"]


def test_add_id_exists(sql_gateway, obj_in_db):
    with pytest.raises(
        AlreadyExists, match=f"record with id={obj_in_db['id']} already exists"
    ):
        sql_gateway.add(obj_in_db)


@pytest.mark.parametrize("id", [10, None, "delete"])
def test_add_integrity_error(sql_gateway, obj, id):
    obj.pop("t")  # will cause the IntegrityError
    if id != "delete":
        obj["id"] = id
    with pytest.raises(NotNullViolationError):
        sql_gateway.add(obj)


def test_add_unkown_column(sql_gateway, obj):
    created = sql_gateway.add({"unknown": "foo", **obj})

    created.pop("id")
    assert created == obj


def test_update(sql_gateway, test_transaction, obj_in_db):
    obj_in_db["t"] = "bar"

    updated = sql_gateway.update(obj_in_db)

    assert updated is not obj_in_db
    assert updated == obj_in_db

    res = test_transaction.execute(
        text(f"SELECT * FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["t"] == "bar"


def test_update_not_found(sql_gateway, obj):
    obj["id"] = 42

    with pytest.raises(DoesNotExist):
        sql_gateway.update(obj)


def test_update_unkown_column(sql_gateway, obj_in_db):
    obj_in_db["t"] = "bar"
    updated = sql_gateway.update({"unknown": "foo", **obj_in_db})

    assert updated == obj_in_db


def test_upsert_does_add(sql_gateway, test_transaction, obj):
    obj["id"] = 42
    created = sql_gateway.upsert(obj)

    assert created is not obj
    assert created == obj

    res = test_transaction.execute(text("SELECT * FROM test_model WHERE id = 42"))
    assert res[0]["t"] == obj["t"]


def test_upsert_does_update(sql_gateway, test_transaction, obj_in_db):
    obj_in_db["t"] = "bar"
    updated = sql_gateway.upsert(obj_in_db)

    assert updated is not obj_in_db
    assert updated == obj_in_db

    res = test_transaction.execute(
        text(f"SELECT * FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["t"] == "bar"


def test_upsert_no_id(sql_gateway, test_transaction, obj):
    with mock.patch.object(sql_gateway, "add", new_callable=mock.AsyncMock) as add_m:
        created = sql_gateway.upsert(obj)
        add_m.assert_awaited_with(obj)
        assert created == add_m.return_value


def test_remove(sql_gateway, test_transaction, obj_in_db):
    assert sql_gateway.remove(obj_in_db["id"])

    res = test_transaction.execute(
        text(f"SELECT COUNT(*) FROM test_model WHERE id = {obj_in_db['id']}")
    )
    assert res[0]["count"] == 0


def test_remove_not_found(sql_gateway):
    assert not sql_gateway.remove(42)


def test_update_if_unmodified_since(sql_gateway, obj_in_db):
    obj_in_db["t"] = "bar"

    updated = sql_gateway.update(obj_in_db, if_unmodified_since=obj_in_db["updated_at"])

    assert updated == obj_in_db


@pytest.mark.parametrize(
    "if_unmodified_since", [datetime.now(timezone.utc), datetime(2010, 1, 1)]
)
def test_update_if_unmodified_since_not_ok(sql_gateway, obj_in_db, if_unmodified_since):
    obj_in_db["t"] = "bar"

    with pytest.raises(Conflict):
        sql_gateway.update(obj_in_db, if_unmodified_since=if_unmodified_since)


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
def test_filter(filters, match, sql_gateway, obj_in_db):
    actual = sql_gateway.filter(filters)

    assert actual == ([obj_in_db] if match else [])


@pytest.fixture
def obj2_in_db(test_transaction, obj):
    res = test_transaction.execute(
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
def test_count(filters, expected, sql_gateway, obj_in_db, obj2_in_db):
    actual = sql_gateway.count(filters)
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
def test_exists(filters, expected, sql_gateway, obj_in_db, obj2_in_db):
    actual = sql_gateway.exists(filters)
    assert actual == expected


def test_truncate(database: SQLDatabase, obj):
    gateway = TstSQLGateway(database)
    gateway.add(obj)
    assert gateway.exists([])
    database.truncate_tables(["test_model"])
    assert not gateway.exists([])
