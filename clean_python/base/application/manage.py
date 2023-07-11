# (c) Nelen & Schuurmans

from typing import Any
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

from clean_python.base.domain import Filter
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

    async def retrieve(self, id: int) -> T:
        return await self.repo.get(id)

    async def create(self, values: Json) -> T:
        return await self.repo.add(values)

    async def update(self, id: int, values: Json) -> T:
        return await self.repo.update(id, values)

    async def destroy(self, id: int) -> bool:
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
