# This module is a copy paste of test_sql_gateway.py
from datetime import datetime
from datetime import timezone

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import text

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python.sql import SyncSQLGateway
from clean_python.sql.sqlalchemy_sync_sql_database import SQLAlchemySyncSQLDatabase

from .sql_model import test_model


@pytest.fixture(scope="session")
def database(postgres_db_url):
    # pool_size=2 for Conflict test
    db = SQLAlchemySyncSQLDatabase(postgres_db_url, pool_size=2)
    yield db
    db.dispose()


@pytest.fixture
def test_transaction(database):
    with database.testing_transaction() as test_transaction:
        yield test_transaction


class TstSQLGateway(SyncSQLGateway, table=test_model):
    pass


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
        "json": {"foo": 2},
    }


@pytest.fixture
def obj_in_db(test_transaction, obj):
    res = test_transaction.execute(
        text(
            "INSERT INTO test_model (t, f, b, updated_at, json) "
            "VALUES ('foo', 1.23, TRUE, '2016-06-22 19:10:25-07', '{\"foo\"\\:2}') "
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
    with pytest.raises(IntegrityError):
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


def test_truncate(database, obj):
    gateway = TstSQLGateway(database)
    gateway.add(obj)
    assert gateway.exists([])
    database.truncate_tables(["test_model"])
    assert not gateway.exists([])
