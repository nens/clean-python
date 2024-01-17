import re
from abc import ABC
from abc import abstractmethod
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

import asyncpg
from async_lru import alru_cache
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect as asyncpg_dialect
from sqlalchemy.sql import Executable

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import Json

__all__ = ["SQLProvider", "SQLDatabase"]


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


class SQLProvider(ABC):
    def compile(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Any, ...]:
        # Rendering SQLAlchemy expressions to SQL, see:
        # - https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html
        compiled = query.compile(
            dialect=DIALECT, compile_kwargs={"render_postcompile": True}
        )
        params = (
            compiled.params
            if bind_params is None
            else {**compiled.params, **bind_params}
        )
        # add params in positional order
        return (str(compiled),) + tuple(params[k] for k in compiled.positiontup)

    @abstractmethod
    async def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
        pass

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["SQLProvider"]:
        raise NotImplementedError()
        yield


class SQLDatabase(SQLProvider):
    def __init__(
        self, url: str, *, isolation_level: str = "repeatable_read", pool_size: int = 1
    ):
        # Note: disable JIT because it amakes the initial queries very slow
        # see https://github.com/MagicStack/asyncpg/issues/530
        if "://" in url:
            url = url.split("://")[1]
        self.url = url
        self.pool_size = pool_size
        self.isolation_level = isolation_level

    @alru_cache
    async def get_pool(self):
        return await asyncpg.create_pool(
            f"postgresql://{self.url}",
            server_settings={"jit": "off"},
            min_size=1,
            max_size=self.pool_size,
        )

    async def dispose(self) -> None:
        pool = await self.get_pool()
        await pool.close()

    async def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
        async with self.transaction() as transaction:
            return await transaction.execute(query, bind_params)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            async with connection.transaction(isolation=self.isolation_level):
                yield SQLTransaction(connection)

    @asynccontextmanager
    async def testing_transaction(self) -> AsyncIterator[SQLProvider]:
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            transaction = connection.transaction()
            await transaction.start()
            try:
                yield SQLTransaction(connection)
            finally:
                await transaction.rollback()

    async def _execute_autocommit(self, query: Executable) -> None:
        pool = await self.get_pool()
        connection: asyncpg.Connection
        async with pool.acquire() as connection:
            await connection.execute(*self.compile(query))

    async def create_database(self, name: str) -> None:
        await self._execute_autocommit(text(f"CREATE DATABASE {name}"))

    async def create_extension(self, name: str) -> None:
        await self._execute_autocommit(text(f"CREATE EXTENSION IF NOT EXISTS {name}"))

    async def drop_database(self, name: str) -> None:
        await self._execute_autocommit(text(f"DROP DATABASE IF EXISTS {name}"))

    async def truncate_tables(self, names: Sequence[str]) -> None:
        quoted = [f'"{x}"' for x in names]
        await self._execute_autocommit(text(f"TRUNCATE TABLE {', '.join(quoted)}"))


class SQLTransaction(SQLProvider):
    def __init__(self, connection: asyncpg.Connection):
        self.connection = connection

    async def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
        try:
            result = await self.connection.fetch(*self.compile(query, bind_params))
        except asyncpg.exceptions.UniqueViolationError as e:
            raise convert_unique_violation_error(e)
        except asyncpg.exceptions.SerializationError:
            raise Conflict("could not execute query due to concurrent update")
        return list(map(dict, result))

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLProvider]:
        async with self.connection.transaction():
            yield self
