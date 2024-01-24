import os

import inject
from sqlalchemy import text

from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v
from clean_python.sql import AsyncpgSQLDatabase
from clean_python.sql import SQLAlchemyAsyncSQLDatabase
from clean_python.sql import SQLAlchemySyncSQLDatabase
from clean_python.sql import SQLDatabase
from clean_python.sql import SyncSQLDatabase

from .application import ManageTestModel
from .application import SyncManageTestModel
from .domain import TestModel
from .domain import TestModelRepository
from .domain import TestModelSyncRepository
from .infrastructure import TestModelGateway
from .infrastructure import TestModelSyncGateway
from .sql_model import create_and_fill_db

URL = "postgres:postgres@localhost:5432"
DB = "sqlcomparison"
POOL_SIZE = 50

SQLDatabaseImpl = {
    "sqlalchemy_sync": SQLAlchemySyncSQLDatabase,
    "sqlalchemy_async": SQLAlchemyAsyncSQLDatabase,
    "asyncpg": AsyncpgSQLDatabase,
}[os.environ["SQL_COMPARISON"]]


USE_SYNC = issubclass(SQLDatabaseImpl, SyncSQLDatabase)


if USE_SYNC:

    def bootstrap(x: inject.Binder) -> None:
        x.bind(SyncSQLDatabase, SQLDatabaseImpl(f"{URL}/{DB}", pool_size=POOL_SIZE))

    class SQLComparisonResource(Resource, version=v(1), name="sql-comparison"):
        def __init__(self):
            self.gateway = TestModelSyncGateway()
            self.manage = SyncManageTestModel(TestModelSyncRepository(self.gateway))

        @get("/sleep/{ms}")
        def sleep(self, ms: int):
            return self.gateway.provider.execute(
                text("SELECT pg_sleep(:sec)").bindparams(sec=ms / 1000)
            )

        @get("/raw/{id}")
        def raw(self, id: int):
            return self.gateway.get(id)

        @get("/get/{id}")
        def get(self, id: int) -> TestModel:
            return self.manage.retrieve(id)

else:

    def bootstrap(x: inject.Binder) -> None:
        x.bind(SQLDatabase, SQLDatabaseImpl(f"{URL}/{DB}", pool_size=POOL_SIZE))

    class SQLComparisonResource(Resource, version=v(1), name="sql-comparison"):
        def __init__(self):
            self.gateway = TestModelGateway()
            self.manage = ManageTestModel(TestModelRepository(self.gateway))

        @get("/sleep/{ms}")
        async def sleep(self, ms: int):
            return await self.gateway.provider.execute(
                text("SELECT pg_sleep(:sec)").bindparams(sec=ms / 1000)
            )

        @get("/raw/{id}")
        async def raw(self, id: int):
            return await self.gateway.get(id)

        @get("/get/{id}")
        async def get(self, id: int) -> TestModel:
            return await self.manage.retrieve(id)


app = Service(SQLComparisonResource()).create_app(
    title="sql-comparison",
    description="Comparison for SQL providers",
    hostname="localhost",
    on_startup=[
        lambda: create_and_fill_db(URL, DB),
        lambda: inject.configure(bootstrap),
    ],
)
