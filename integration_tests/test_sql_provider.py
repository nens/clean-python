# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import pytest
from sqlalchemy.sql import text

from clean_python.sql import SQLDatabase

count_query = text("SELECT COUNT(*) FROM test_model")
insert_query = text(
    "INSERT INTO test_model (t, f, b, updated_at) "
    "VALUES ('foo', 1.23, TRUE, '2016-06-22 19:10:25-07') "
    "RETURNING id"
)


@pytest.fixture
async def provider(postgres_url):
    provider = SQLDatabase(postgres_url)
    await provider.execute(text("DELETE FROM test_model WHERE TRUE RETURNING id"))
    yield provider
    await provider.execute(text("DELETE FROM test_model WHERE TRUE RETURNING id"))


@pytest.fixture
async def testing_provider(provider):
    async with provider.transaction() as nested:
        yield nested


async def test_execute(provider):
    await provider.execute(insert_query)
    assert await provider.execute(count_query) == [{"count": 1}]


async def test_transaction_commits(provider):
    async with provider.transaction() as trans:
        await trans.execute(insert_query)

    assert await provider.execute(count_query) == [{"count": 1}]


async def test_transaction_err(provider):
    await provider.execute(insert_query)

    with pytest.raises(RuntimeError):
        async with provider.transaction() as trans:
            await trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert await provider.execute(count_query) == [{"count": 1}]


async def test_testing_transaction(provider):
    async with provider.testing_transaction() as trans:
        await trans.execute(insert_query)

    assert await provider.execute(count_query) == [{"count": 0}]


async def test_testing_execute(testing_provider):
    await testing_provider.execute(insert_query)
    assert await testing_provider.execute(count_query) == [{"count": 1}]


async def test_testing_transaction_commits(testing_provider):
    async with testing_provider.transaction() as trans:
        await trans.execute(insert_query)

    assert await testing_provider.execute(count_query) == [{"count": 1}]


async def test_testing_transaction_err(testing_provider):
    await testing_provider.execute(insert_query)

    with pytest.raises(RuntimeError):
        async with testing_provider.transaction() as trans:
            await trans.execute(insert_query)

            raise RuntimeError()  # triggers rollback

    assert await testing_provider.execute(count_query) == [{"count": 1}]
