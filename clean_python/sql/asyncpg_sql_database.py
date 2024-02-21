import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from async_lru import alru_cache
from sqlalchemy.dialects.postgresql.asyncpg import dialect as asyncpg_dialect
from sqlalchemy.sql import Executable

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import Json

from .sql_provider import SQLDatabase
from .sql_provider import SQLProvider

__all__ = ["AsyncpgSQLDatabase"]


UNIQUE_VIOLATION_DETAIL_REGEX = re.compile(
    r"Key\s\((?P<key>.*)\)=\((?P<value>.*)\)\s+already exists"
)
DIALECT = asyncpg_dialect()


def convert_unique_violation_error(
    e: asyncpg.exceptions.UniqueViolationError,
) -> AlreadyExists:
    match = UNIQUE_VIOLATION_DETAIL_REGEX.match(e.detail)
    if match:
        return AlreadyExists(key=match["key"], value=match["value"])
    else:
        return AlreadyExists()


def compile(
    query: Executable, bind_params: dict[str, Any] | None = None
) -> tuple[Any, ...]:
    # Rendering SQLAlchemy expressions to SQL, see:
    # - https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html
    # Note that this circumvents the SQLAlchemy caching system and almost certainly
    # will deteriorate performance when using complex query (compared to the
    # standard sqlalchemy .execute)
    compiled = query.compile(
        dialect=DIALECT, compile_kwargs={"render_postcompile": True}
    )
    params = (
        compiled.params if bind_params is None else {**compiled.params, **bind_params}
    )
    # add params in positional order
    return (str(compiled),) + tuple(params[k] for k in compiled.positiontup)


async def init_db_types(conn: asyncpg.Connection):
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


class AsyncpgSQLDatabase(SQLDatabase):
    def __init__(
        self, url: str, *, isolation_level: str = "repeatable_read", pool_size: int = 1
    ):
        self.url = url
        self.pool_size = pool_size
        self.isolation_level = isolation_level

    @alru_cache
    async def get_pool(self):
        # Note: disable JIT because it amakes the initial queries very slow
        # see https://github.com/MagicStack/asyncpg/issues/530
        return await asyncpg.create_pool(
            f"postgresql://{self.url}",
            server_settings={"jit": "off"},
            min_size=1,
            max_size=self.pool_size,
            init=init_db_types,
        )

    async def dispose(self) -> None:
        pool = await self.get_pool()
        await pool.close()

    async def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        # compile before acquiring the connection
        args = compile(query, bind_params)
        pool = await self.get_pool()
        try:
            result = await pool.fetch(*args)
        except asyncpg.exceptions.UniqueViolationError as e:
            raise convert_unique_violation_error(e)
        except asyncpg.exceptions.SerializationError:
            raise Conflict("could not execute query due to concurrent update")
        return list(map(dict, result))

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            async with connection.transaction(isolation=self.isolation_level):
                yield AsyncpgSQLTransaction(connection)

    @asynccontextmanager
    async def testing_transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            transaction = connection.transaction()
            await transaction.start()
            try:
                yield AsyncpgSQLTransaction(connection)
            finally:
                await transaction.rollback()

    async def execute_autocommit(self, query: Executable) -> None:
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            await connection.execute(*compile(query))


class AsyncpgSQLTransaction(SQLProvider):
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        try:
            result = await self.connection.fetch(*compile(query, bind_params))
        except asyncpg.exceptions.UniqueViolationError as e:
            raise convert_unique_violation_error(e)
        except asyncpg.exceptions.SerializationError:
            raise Conflict("could not execute query due to concurrent update")
        return list(map(dict, result))

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:  # type: ignore
        async with self.connection.transaction():
            yield self
