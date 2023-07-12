from abc import ABC
from abc import abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator
from typing import List

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import Executable

from clean_python import Conflict
from clean_python import Json

__all__ = ["SQLProvider", "SQLDatabase"]


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
                yield SQLTransaction(connection)
                await transaction.rollback()

    async def _execute_autocommit(self, query: Executable) -> None:
        engine = create_async_engine(self.engine.url, isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            await connection.execute(query)

    async def create_database(self, name: str) -> None:
        await self._execute_autocommit(text(f"CREATE DATABASE {name}"))

    async def drop_database(self, name: str) -> None:
        await self._execute_autocommit(text(f"DROP DATABASE IF EXISTS {name}"))


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

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:
        async with self.connection.begin_nested():
            yield self
