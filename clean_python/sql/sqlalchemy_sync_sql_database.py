import re
from contextlib import contextmanager
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import Executable

from clean_python import AlreadyExists
from clean_python import Conflict
from clean_python import Json

from .sql_provider import SyncSQLDatabase
from .sql_provider import SyncSQLProvider

__all__ = ["SQLAlchemySyncSQLDatabase"]


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
        match = UNIQUE_VIOLATION_DETAIL_REGEX.match(e.orig.args[0].split("\n")[-1])
        if match:
            raise AlreadyExists(key=match["key"], value=match["value"])
        else:
            raise AlreadyExists()


class SQLAlchemySyncSQLDatabase(SyncSQLDatabase):
    engine: Engine

    def __init__(self, url: str, **kwargs):
        kwargs.setdefault("isolation_level", "REPEATABLE READ")
        self.url = url
        self.engine = create_engine(f"postgresql://{url}", **kwargs)

    def dispose(self) -> None:
        self.engine.dispose()

    def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
        with self.transaction() as transaction:
            return transaction.execute(query, bind_params)

    @contextmanager
    def transaction(self) -> Iterator[SyncSQLProvider]:
        with self.engine.connect() as connection:
            with connection.begin():
                yield SQLAlchemySyncSQLTransaction(connection)

    def execute_autocommit(self, query: Executable) -> None:
        engine = create_engine(f"postgresql://{self.url}", isolation_level="AUTOCOMMIT")
        with engine.connect() as connection:
            connection.execute(query)
        engine.dispose()


class SQLAlchemySyncSQLTransaction(SyncSQLProvider):
    def __init__(self, connection: Connection):
        self.connection = connection

    def execute(
        self, query: Executable, bind_params: Optional[Dict[str, Any]] = None
    ) -> List[Json]:
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
    def transaction(self) -> Iterator[SyncSQLProvider]:
        with self.connection.begin_nested():
            yield self
