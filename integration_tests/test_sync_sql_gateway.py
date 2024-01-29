# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans
from datetime import datetime
from datetime import timezone

import pytest
from sqlalchemy.sql import text

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
