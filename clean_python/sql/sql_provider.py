from collections.abc import AsyncIterator
from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any

from sqlalchemy import text
from sqlalchemy.sql import Executable

from clean_python import Json

__all__ = ["SQLProvider", "SQLDatabase", "SyncSQLProvider", "SyncSQLDatabase"]


class SQLProvider:
    async def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        raise NotImplementedError()

    async def transaction(self) -> AsyncIterator["SQLProvider"]:
        raise NotImplementedError()
        yield

    async def testing_transaction(self) -> AsyncIterator["SQLProvider"]:
        raise NotImplementedError()
        yield


class SQLDatabase(SQLProvider):
    async def execute_autocommit(self, query: Executable) -> None:
        pass

    async def create_database(self, name: str) -> None:
        await self.execute_autocommit(text(f"CREATE DATABASE {name}"))

    async def create_extension(self, name: str) -> None:
        await self.execute_autocommit(text(f"CREATE EXTENSION IF NOT EXISTS {name}"))

    async def drop_database(self, name: str) -> None:
        await self.execute_autocommit(text(f"DROP DATABASE IF EXISTS {name}"))

    async def truncate_tables(self, names: Sequence[str]) -> None:
        quoted = [f'"{x}"' for x in names]
        await self.execute_autocommit(text(f"TRUNCATE TABLE {', '.join(quoted)}"))


class SyncSQLProvider:
    def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        raise NotImplementedError()

    def transaction(self) -> Iterator["SyncSQLProvider"]:
        raise NotImplementedError()
        yield

    def testing_transaction(self) -> Iterator["SyncSQLProvider"]:
        raise NotImplementedError()
        yield


class SyncSQLDatabase(SyncSQLProvider):
    def execute_autocommit(self, query: Executable) -> None:
        pass

    def create_database(self, name: str) -> None:
        self.execute_autocommit(text(f"CREATE DATABASE {name}"))

    def create_extension(self, name: str) -> None:
        self.execute_autocommit(text(f"CREATE EXTENSION IF NOT EXISTS {name}"))

    def drop_database(self, name: str) -> None:
        self.execute_autocommit(text(f"DROP DATABASE IF EXISTS {name}"))

    def truncate_tables(self, names: Sequence[str]) -> None:
        quoted = [f'"{x}"' for x in names]
        self.execute_autocommit(text(f"TRUNCATE TABLE {', '.join(quoted)}"))
