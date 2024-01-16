from async_lru import alru_cache

from clean_python import Json


class SQLProvider:
    def __init__(self, url: str):
        self.url = url  # something like scott:tiger@localhost/test"
        self._initialized = False
        # easy way of caching init thread-safe
        self.maybe_init = alru_cache(self.init)

    async def init(self) -> None:
        raise NotImplementedError()

    async def execute(self, query: str) -> list[Json]:
        raise NotImplementedError()

    async def init_and_execute(self, query: str) -> list[Json]:
        await self.maybe_init()
        return await self.execute(query)
