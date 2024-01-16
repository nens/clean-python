import asyncpg

from clean_python import Json

from .base_provider import SQLProvider


class AsyncpgProvider(SQLProvider):
    async def init(self) -> None:
        # Note: disable JIT because it amakes the initial queries very slow
        # see https://github.com/MagicStack/asyncpg/issues/530
        self.pool = await asyncpg.create_pool(
            f"postgresql://{self.url}",
            server_settings={"jit": "off"},
            min_size=5,
            max_size=5,
        )

    async def execute(self, query: str) -> list[Json]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.fetch(query)
                return [{"result": list(x)} for x in result]
