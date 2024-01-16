from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from clean_python import Json

from .base_provider import SQLProvider


class AsyncSQLAlchemyProvider(SQLProvider):
    async def init(self) -> None:
        self.engine = create_async_engine(f"postgresql+asyncpg://{self.url}")

    async def execute(self, query: str) -> list[Json]:
        async with self.engine.begin() as conn:
            result = await conn.execute(text(query))
            return [{"result": list(x)} for x in result.fetchall()]


class SyncSQLAlchemyProvider(SQLProvider):
    async def init(self) -> None:
        self.engine = create_engine(f"postgresql+psycopg2://{self.url}")

    async def execute(self, query: str) -> list[Json]:
        # we happily block the event loop here
        with self.engine.begin() as conn:
            result = conn.execute(text(query))
            return [{"result": list(x)} for x in result.fetchall()]
