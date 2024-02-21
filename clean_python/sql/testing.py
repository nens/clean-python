from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import asynccontextmanager
from contextlib import contextmanager
from typing import Any
from unittest import mock

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Executable

from clean_python import Json
from clean_python.sql import SQLProvider
from clean_python.sql import SyncSQLProvider

__all__ = ["FakeSQLDatabase", "FakeSyncSQLDatabase", "assert_query_equal"]


DIALECT = postgresql.dialect()


class FakeSQLDatabase(SQLProvider):
    def __init__(self):
        self.queries: list[list[Executable]] = []
        self.result = mock.Mock(return_value=[])

    async def execute(
        self, query: Executable, _: dict[str, Any] | None = None
    ) -> list[Json]:
        self.queries.append([query])
        return self.result()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["SQLProvider"]:  # type: ignore
        x = FakeSQLTransaction(result=self.result)
        self.queries.append(x.queries)
        yield x


class FakeSQLTransaction(SQLProvider):
    def __init__(self, result: mock.Mock):
        self.queries: list[Executable] = []
        self.result = result

    async def execute(
        self, query: Executable, _: dict[str, Any] | None = None
    ) -> list[Json]:
        self.queries.append(query)
        return self.result()


class FakeSyncSQLDatabase(SyncSQLProvider):
    def __init__(self):
        self.queries: list[list[Executable]] = []
        self.result = mock.Mock(return_value=[])

    def execute(self, query: Executable, _: dict[str, Any] | None = None) -> list[Json]:
        self.queries.append([query])
        return self.result()

    @contextmanager
    def transaction(self) -> Iterator["SyncSQLProvider"]:  # type: ignore
        x = FakeSyncSQLTransaction(result=self.result)
        self.queries.append(x.queries)
        yield x


class FakeSyncSQLTransaction(SyncSQLProvider):
    def __init__(self, result: mock.Mock):
        self.queries: list[Executable] = []
        self.result = result

    def execute(self, query: Executable, _: dict[str, Any] | None = None) -> list[Json]:
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
