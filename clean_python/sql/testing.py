from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Dict
from typing import List
from typing import Optional
from unittest import mock

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Executable

from clean_python import Json
from clean_python.sql import SQLProvider

__all__ = ["FakeSQLDatabase", "assert_query_equal"]


class FakeSQLDatabase(SQLProvider):
    def __init__(self):
        self.queries: List[List[Executable]] = []
        self.result = mock.Mock(return_value=[])

    async def execute(
        self, query: Executable, _: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
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

    async def execute(
        self, query: Executable, _: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
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
