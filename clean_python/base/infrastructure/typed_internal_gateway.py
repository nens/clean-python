# (c) Nelen & Schuurmans
from abc import abstractmethod
from abc import abstractproperty
from typing import Generic
from typing import List
from typing import Optional
from typing import TypeVar

from clean_python.base.application.manage import Manage
from clean_python.base.domain import BadRequest
from clean_python.base.domain import DoesNotExist
from clean_python.base.domain import Filter
from clean_python.base.domain import Json
from clean_python.base.domain import PageOptions
from clean_python.base.domain import RootEntity
from clean_python.base.domain import ValueObject

__all__ = ["TypedInternalGateway"]


E = TypeVar("E", bound=RootEntity)  # External
T = TypeVar("T", bound=ValueObject)  # Internal


# don't subclass Gateway; Gateway makes Json objects
class TypedInternalGateway(Generic[E, T]):
    @abstractproperty
    def manage(self) -> Manage[E]:
        raise NotImplementedError()

    @abstractmethod
    def _map(self, obj: E) -> T:
        raise NotImplementedError()

    async def get(self, id: int) -> Optional[T]:
        try:
            result = await self.manage.retrieve(id)
        except DoesNotExist:
            return None
        else:
            return self._map(result)

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[T]:
        page = await self.manage.filter(filters, params)
        return [self._map(x) for x in page.items]

    async def add(self, item: T) -> T:
        try:
            created = await self.manage.create(item.model_dump())
        except BadRequest as e:
            raise ValueError(e)
        return self._map(created)

    async def remove(self, id) -> bool:
        return await self.manage.destroy(id)

    async def count(self, filters: List[Filter]) -> int:
        return await self.manage.count(filters)

    async def exists(self, filters: List[Filter]) -> bool:
        return await self.manage.exists(filters)

    async def update(self, values: Json) -> T:
        values = values.copy()
        id_ = values.pop("id", None)
        if id_ is None:
            raise DoesNotExist("item", id_)
        try:
            updated = await self.manage.update(id_, values)
        except BadRequest as e:
            raise ValueError(e)
        return self._map(updated)
