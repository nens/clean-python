from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from sqlalchemy import text
from sqlalchemy.sql import Executable

from clean_python import Json

__all__ = ["SQLProvider", "SQLDatabase"]


class SQLProvider:
    async def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
        raise NotImplementedError()

    async def transaction(self) -> AsyncIterator["SQLProvider"]:
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
