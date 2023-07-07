from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator, List
from unittest import mock

from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.sql import Executable

from clean_python.base.domain.exceptions import Conflict
from clean_python.base.infrastructure.gateway import Json

__all__ = ["SQLProvider", "SQLDatabase", "FakeSQLDatabase", "assert_query_equal"]


def is_serialization_error(e: DBAPIError) -> bool:
    return e.orig.args[0].startswith("<class 'asyncpg.exceptions.SerializationError'>")


class SQLProvider(ABC):
    @abstractmethod
    async def execute(self, query: Executable) -> List[Json]:
        pass

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["SQLProvider"]:
        raise NotImplementedError()
        yield


class SQLDatabase(SQLProvider):
    engine: AsyncEngine

    def __init__(self, url: str, **kwargs):
        kwargs.setdefault("isolation_level", "READ COMMITTED")
        self.engine = create_async_engine(url, **kwargs)

    async def dispose(self) -> None:
        await self.engine.dispose()

    def dispose_sync(self) -> None:
        self.engine.sync_engine.dispose()

    async def execute(self, query: Executable) -> List[Json]:
        async with self.transaction() as transaction:
            return await transaction.execute(query)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:
        async with self.engine.connect() as connection:
            async with connection.begin():
                yield SQLTransaction(connection)

    @asynccontextmanager
    async def testing_transaction(self) -> AsyncIterator[SQLProvider]:
        async with self.engine.connect() as connection:
            async with connection.begin() as transaction:
                yield SQLTestTransaction(connection)
                await transaction.rollback()


class SQLTransaction(SQLProvider):
    def __init__(self, connection: AsyncConnection):
        self.connection = connection

    async def execute(self, query: Executable) -> List[Json]:
        try:
            result = await self.connection.execute(query)
        except DBAPIError as e:
            if is_serialization_error(e):
                raise Conflict(str(e))
            else:
                raise e
        # _asdict() is a documented method of a NamedTuple
        # https://docs.python.org/3/library/collections.html#collections.somenamedtuple._asdict
        return [x._asdict() for x in result.fetchall()]


class SQLTestTransaction(SQLTransaction):
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:
        async with self.connection.begin_nested():
            yield self


class FakeSQLDatabase(SQLProvider):
    def __init__(self):
        self.queries: List[List[Executable]] = []
        self.result = mock.Mock(return_value=[])

    async def execute(self, query: Executable) -> List[Json]:
        self.queries.append([query])
        return self.result()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["SQLProvider"]:
        x = FakeSQLTransaction(result=self.result)
        self.queries.append(x.queries)
        yield x


class FakeSQLTransaction(SQLProvider):
    def __init__(self, result: mock.Mock):
        self.queries: List[Executable] = []
        self.result = result

    async def execute(self, query: Executable) -> List[Json]:
        self.queries.append(query)
        return self.result()


def assert_query_equal(q: Executable, expected: str, literal_binds: bool = True):
    """There are two ways of 'binding' parameters (for testing!):

    literal_binds=True: use the built-in sqlalchemy way, which fails on some datatypes (Range)
    literal_binds=False: do it yourself using %, there is no 'mogrify' so don't expect quotes.
    """
    assert isinstance(q, Executable)
    compiled = q.compile(
        compile_kwargs={"literal_binds": literal_binds},
        dialect=postgresql.dialect(),
    )
    if not literal_binds:
        actual = str(compiled) % compiled.params
    else:
        actual = str(compiled)
    actual = actual.replace("\n", "").replace("  ", " ")
    assert actual == expected
