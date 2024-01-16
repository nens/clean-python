from enum import Enum

from clean_python import NullGateway
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v

from .sqlalchemy_provider import AsyncSQLAlchemyProvider
from .sqlalchemy_provider import SyncSQLAlchemyProvider

URL = "postgres:postgres@localhost:5432"


class ProviderOptions(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    SQLALCHEMY_SYNC = "sqlalchemy_sync"


class SQL(Resource, version=v(1), name="sql"):
    def __init__(self):
        self.providers = {
            ProviderOptions.SQLALCHEMY: AsyncSQLAlchemyProvider(URL),
            ProviderOptions.SQLALCHEMY_SYNC: SyncSQLAlchemyProvider(URL),
        }

    @get("/{provider}/{wait_time_ms}")
    async def sqlalchemy_async(self, provider: ProviderOptions, wait_time_ms: int):
        return await self.providers[provider].init_and_execute(
            f"SELECT pg_sleep({wait_time_ms} / 1000)"
        )


service = Service(SQL())

app = service.create_app(
    title="SQL test service",
    description="Service for testing various sql drivers",
    hostname="testserver",
    access_logger_gateway=NullGateway(),
)
