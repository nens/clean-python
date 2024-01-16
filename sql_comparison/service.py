from enum import Enum
from pathlib import Path

from clean_python import NullGateway
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v
from clean_python.testing.profyle_fastapi_profiler import ProfyleMiddleware
from clean_python.testing.profyle_fastapi_profiler import ProfyleSettings

from .asyncpg_provider import AsyncpgProvider
from .sqlalchemy_provider import AsyncSQLAlchemyProvider
from .sqlalchemy_provider import NullPoolSQLAlchemyProvider
from .sqlalchemy_provider import SyncSQLAlchemyProvider

URL = "postgres:postgres@localhost:5432"


class ProviderOptions(str, Enum):
    SQLALCHEMY = "sqlalchemy"
    SQLALCHEMY_NULLPOOL = "sqlalchemy_nullpool"
    SQLALCHEMY_SYNC = "sqlalchemy_sync"
    ASYNCPG = "asyncpg"


class SQL(Resource, version=v(1), name="sql"):
    def __init__(self):
        self.providers = {
            ProviderOptions.SQLALCHEMY: AsyncSQLAlchemyProvider(URL),
            ProviderOptions.SQLALCHEMY_NULLPOOL: NullPoolSQLAlchemyProvider(URL),
            ProviderOptions.SQLALCHEMY_SYNC: SyncSQLAlchemyProvider(URL),
            ProviderOptions.ASYNCPG: AsyncpgProvider(URL),
        }

    @get("/health")
    async def health(self):
        return "ok"

    @get("/{provider}/{wait_time_ms}")
    async def sql(self, provider: ProviderOptions, wait_time_ms: int):
        return await self.providers[provider].init_and_execute(
            f"SELECT pg_sleep({wait_time_ms / 1000})"
        )

    @get("/{provider}/{wait_time_ms}/profile")
    async def sql_profile(self, provider: ProviderOptions, wait_time_ms: int):
        return await self.providers[provider].init_and_execute(
            f"SELECT pg_sleep({wait_time_ms / 1000})"
        )


service = Service(SQL())

app = service.create_app(
    title="SQL test service",
    description="Service for testing various sql drivers",
    hostname="testserver",
    access_logger_gateway=NullGateway(),
)
try:
    import profyle  # NOQA

    app.add_middleware(
        ProfyleMiddleware,
        settings=ProfyleSettings(
            db_path=Path(__file__).parent / "profile.sqlite", pattern="*/profile"
        ),
    )
except ImportError:
    pass
