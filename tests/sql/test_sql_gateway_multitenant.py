import pytest
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text

from clean_python import ctx
from clean_python import Filter
from clean_python import Tenant
from clean_python.sql import SQLGateway
from clean_python.sql.testing import assert_query_equal
from clean_python.sql.testing import FakeSQLDatabase

writer = Table(
    "writer",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", Text, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("tenant", Integer, nullable=False),
)


ALL_FIELDS = "writer.id, writer.value, writer.updated_at, writer.tenant"


class TstSQLGateway(SQLGateway, table=writer, multitenant=True):
    pass


@pytest.fixture
def sql_gateway():
    return TstSQLGateway(FakeSQLDatabase())


@pytest.fixture
def tenant():
    ctx.tenant = Tenant(id=2, name="foo")
    return ctx.tenant


async def test_no_tenant(sql_gateway):
    with pytest.raises(RuntimeError):
        await sql_gateway.filter([])
    assert len(sql_gateway.provider.queries) == 0


async def test_missing_tenant_column():
    table = Table(
        "notenant",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=True),
    )

    with pytest.raises(ValueError):

        class Foo(SQLGateway, table=table, multitenant=True):
            pass


async def test_filter(sql_gateway, tenant):
    sql_gateway.provider.result.return_value = [{"id": 2, "value": "foo"}]
    assert await sql_gateway.filter([Filter(field="id", values=[1])]) == [
        {"id": 2, "value": "foo"}
    ]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT {ALL_FIELDS} FROM writer WHERE writer.id = 1 AND writer.tenant = {tenant.id}",
    )


async def test_count(sql_gateway, tenant):
    sql_gateway.provider.result.return_value = [{"count": 4}]
    assert await sql_gateway.count([Filter(field="id", values=[1])]) == 4
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        f"SELECT count(*) AS count FROM writer WHERE writer.id = 1 AND writer.tenant = {tenant.id}",
    )


async def test_add(sql_gateway, tenant):
    records = [{"id": 2, "value": "foo", "tenant": tenant.id}]
    sql_gateway.provider.result.return_value = records
    assert await sql_gateway.add({"value": "foo"}) == records[0]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"INSERT INTO writer (value, tenant) VALUES ('foo', {tenant.id}) RETURNING {ALL_FIELDS}"
        ),
    )


async def test_update(sql_gateway, tenant):
    records = [{"id": 2, "value": "foo", "tenant": tenant.id}]
    sql_gateway.provider.result.return_value = records
    assert await sql_gateway.update({"id": 2, "value": "foo"}) == records[0]
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"UPDATE writer SET id=2, value='foo', tenant={tenant.id} "
            f"WHERE writer.id = 2 AND writer.tenant = {tenant.id} "
            f"RETURNING {ALL_FIELDS}"
        ),
    )


async def test_remove(sql_gateway, tenant):
    sql_gateway.provider.result.return_value = [{"id": 2}]
    assert (await sql_gateway.remove(2)) is True
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"DELETE FROM writer WHERE writer.id = 2 AND writer.tenant = {tenant.id} "
            f"RETURNING writer.id"
        ),
    )


async def test_upsert(sql_gateway, tenant):
    record = {"id": 2, "value": "foo", "tenant": tenant.id}
    sql_gateway.provider.result.return_value = [record]
    assert await sql_gateway.upsert({"id": 2, "value": "foo"}) == record
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"INSERT INTO writer (id, value, tenant) VALUES (2, 'foo', {tenant.id}) "
            f"ON CONFLICT (id, tenant) DO UPDATE SET "
            f"id = %(param_1)s, value = %(param_2)s, tenant = %(param_3)s "
            f"RETURNING {ALL_FIELDS}"
        ),
    )


async def test_update_transactional(sql_gateway, tenant):
    existing = {"id": 2, "value": "foo", "tenant": tenant.id}
    expected = {"id": 2, "value": "bar", "tenant": tenant.id}
    sql_gateway.provider.result.side_effect = ([existing], [expected])
    actual = await sql_gateway.update_transactional(
        2, lambda x: {"id": x["id"], "value": "bar"}
    )
    assert actual == expected

    (queries,) = sql_gateway.provider.queries
    assert len(queries) == 2
    assert_query_equal(
        queries[0],
        (
            f"SELECT {ALL_FIELDS} FROM writer WHERE writer.id = 2 "
            f"AND writer.tenant = {tenant.id} FOR UPDATE"
        ),
    )
    assert_query_equal(
        queries[1],
        (
            f"UPDATE writer SET id=2, value='bar', tenant={tenant.id} "
            f"WHERE writer.id = 2 AND writer.tenant = {tenant.id} RETURNING {ALL_FIELDS}"
        ),
    )


async def test_exists(sql_gateway, tenant):
    sql_gateway.provider.result.return_value = [{"exists": True}]
    assert await sql_gateway.exists([Filter(field="id", values=[1])]) is True
    assert len(sql_gateway.provider.queries) == 1
    assert_query_equal(
        sql_gateway.provider.queries[0][0],
        (
            f"SELECT true AS exists FROM writer "
            f"WHERE writer.id = 1 AND writer.tenant = {tenant.id} LIMIT 1"
        ),
    )
