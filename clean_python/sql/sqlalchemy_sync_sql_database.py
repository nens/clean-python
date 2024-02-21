import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import Executable

from clean_python import Json

from .sql_provider import SyncSQLDatabase
from .sql_provider import SyncSQLProvider
from .sqlalchemy_async_sql_database import maybe_raise_already_exists
from .sqlalchemy_async_sql_database import maybe_raise_conflict

__all__ = ["SQLAlchemySyncSQLDatabase"]


UNIQUE_VIOLATION_DETAIL_REGEX = re.compile(
    r"DETAIL:\s*Key\s\((?P<key>.*)\)=\((?P<value>.*)\)\s+already exists"
)


class SQLAlchemySyncSQLDatabase(SyncSQLDatabase):
    engine: Engine

    def __init__(self, url: str, **kwargs):
        kwargs.setdefault("isolation_level", "REPEATABLE READ")
        self.url = url
        self.engine = create_engine(f"postgresql://{url}", **kwargs)

    def dispose(self) -> None:
        self.engine.dispose()

    def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        with self.transaction() as transaction:
            return transaction.execute(query, bind_params)

    @contextmanager
    def transaction(self) -> Iterator[SyncSQLProvider]:  # type: ignore
        with self.engine.connect() as connection:
            with connection.begin():
                yield SQLAlchemySyncSQLTransaction(connection)

    @contextmanager
    def testing_transaction(self) -> Iterator[SyncSQLProvider]:  # type: ignore
        with self.engine.connect() as connection:
            with connection.begin() as transaction:
                yield SQLAlchemySyncSQLTransaction(connection)
                transaction.rollback()

    def execute_autocommit(self, query: Executable) -> None:
        engine = create_engine(f"postgresql://{self.url}", isolation_level="AUTOCOMMIT")
        with engine.connect() as connection:
            connection.execute(query)
        engine.dispose()


class SQLAlchemySyncSQLTransaction(SyncSQLProvider):
    def __init__(self, connection: Connection):
        self.connection = connection

    def execute(
        self, query: Executable, bind_params: dict[str, Any] | None = None
    ) -> list[Json]:
        try:
            result = self.connection.execute(query, bind_params)
        except DBAPIError as e:
            maybe_raise_conflict(e)
            maybe_raise_already_exists(e)
            raise e
        # _asdict() is a documented method of a NamedTuple
        # https://docs.python.org/3/library/collections.html#collections.somenamedtuple._asdict
        return [x._asdict() for x in result.fetchall()]

    @contextmanager
    def transaction(self) -> Iterator[SyncSQLProvider]:  # type: ignore
        with self.connection.begin_nested():
            yield self
