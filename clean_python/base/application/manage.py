# (c) Nelen & Schuurmans

from typing import Any
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

import backoff

from clean_python.base.domain import Conflict
from clean_python.base.domain import Filter
from clean_python.base.domain import Id
from clean_python.base.domain import Json
from clean_python.base.domain import Page
from clean_python.base.domain import PageOptions
from clean_python.base.domain import Repository
from clean_python.base.domain import RootEntity

T = TypeVar("T", bound=RootEntity)

__all__ = ["Manage"]


class Manage(Generic[T]):
    repo: Repository[T]
    entity: Type[T]

    def __init__(self, repo: Optional[Repository[T]] = None):
        assert repo is not None
        self.repo = repo

    def __init_subclass__(cls) -> None:
        (base,) = cls.__orig_bases__  # type: ignore
        (entity,) = base.__args__
        assert issubclass(entity, RootEntity)
        super().__init_subclass__()
        cls.entity = entity

    async def retrieve(self, id: Id) -> T:
        return await self.repo.get(id)

    async def create(self, values: Json) -> T:
        return await self.repo.add(values)

    async def update(self, id: Id, values: Json, retry_on_conflict: bool = True) -> T:
        """This update has a built-in retry function that can be switched off.

        This because some gateways (SQLGateway, ApiGateway) may raise Conflict
        errors in case there are concurrency issues. The backoff strategy assumes that
        we can retry immediately (because the conflict is gone immediately), but it
        does add some jitter between 0 and 200 ms to avoid many competing processes.

        If the repo.update is not idempotent (which is atypical), retries should be
        switched off.
        """
        if retry_on_conflict:
            return await self._update_with_retries(id, values)
        else:
            return await self.repo.update(id, values)

    @backoff.on_exception(backoff.constant, Conflict, max_tries=10, interval=0.2)
    async def _update_with_retries(self, id: Id, values: Json) -> T:
        return await self.repo.update(id, values)

    async def destroy(self, id: Id) -> bool:
        return await self.repo.remove(id)

    async def list(self, params: Optional[PageOptions] = None) -> Page[T]:
        return await self.repo.all(params)

    async def by(
        self, key: str, value: Any, params: Optional[PageOptions] = None
    ) -> Page[T]:
        return await self.repo.by(key, value, params=params)

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> Page[T]:
        return await self.repo.filter(filters, params=params)

    async def count(self, filters: List[Filter]) -> int:
        return await self.repo.count(filters)

    async def exists(self, filters: List[Filter]) -> bool:
        return await self.repo.exists(filters)
