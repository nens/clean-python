import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import Executable

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import Json

from .sql_provider import SQLDatabase
from .sql_provider import SQLProvider

__all__ = ["SQLAlchemyAsyncSQLDatabase"]


UNIQUE_VIOLATION_DETAIL_REGEX = re.compile(
    r"DETAIL:\s*Key\s\((?P<key>.*)\)=\((?P<value>.*)\)\s+already exists"
)


def maybe_raise_conflict(e: DBAPIError) -> None:
    # https://www.postgresql.org/docs/current/errcodes-appendix.html
    if e.orig.pgcode == "40001":  # serialization_failure
        raise Conflict("could not execute query due to concurrent update")


def maybe_raise_already_exists(e: DBAPIError) -> None:
    # https://www.postgresql.org/docs/current/errcodes-appendix.html
    if e.orig.pgcode == "23505":  # unique_violation
        lines = e.orig.args[0].split("\n")
        if len(lines) <= 1:
            raise AlreadyExists()
        match = UNIQUE_VIOLATION_DETAIL_REGEX.match(lines[1])
        if match:
            raise AlreadyExists(key=match["key"], value=match["value"])
        else:
            raise AlreadyExists()


class SQLAlchemyAsyncSQLDatabase(SQLDatabase):
    engine: AsyncEngine

    def __init__(self, url: str, **kwargs):
        kwargs.setdefault("isolation_level", "REPEATABLE READ")
        self.engine = create_async_engine(f"postgresql+asyncpg://{url}", **kwargs)

    async def dispose(self) -> None:
        await self.engine.dispose()

    def dispose_sync(self) -> None:
        self.engine.sync_engine.dispose()

    async def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        async with self.transaction() as transaction:
            return await transaction.execute(query, bind_params)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        async with self.engine.connect() as connection:
            async with connection.begin():
                yield SQLAlchemyAsyncSQLTransaction(connection)

    @asynccontextmanager
    async def testing_transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        async with self.engine.connect() as connection:
            async with connection.begin() as transaction:
                yield SQLAlchemyAsyncSQLTransaction(connection)
                await transaction.rollback()

    async def execute_autocommit(self, query: Executable) -> None:
        engine = create_async_engine(self.engine.url, isolation_level="AUTOCOMMIT")
        async with engine.connect() as connection:
            await connection.execute(query)


class SQLAlchemyAsyncSQLTransaction(SQLProvider):
    def __init__(self, connection: AsyncConnection):
        self.connection = connection

    async def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        try:
            result = await self.connection.execute(query, bind_params)
        except DBAPIError as e:
            maybe_raise_conflict(e)
            maybe_raise_already_exists(e)
            raise e
        # _asdict() is a documented method of a NamedTuple
        # https://docs.python.org/3/library/collections.html#collections.somenamedtuple._asdict
        return [x._asdict() for x in result.fetchall()]

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        async with self.connection.begin_nested():
            yield self
