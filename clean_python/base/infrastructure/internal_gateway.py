# (c) Nelen & Schuurmans
from abc import abstractmethod
from abc import abstractproperty
from datetime import datetime
from typing import Generic
from typing import List
from typing import Optional
from typing import TypeVar

from clean_python.base.application.manage import Manage
from clean_python.base.domain import BadRequest
from clean_python.base.domain import DoesNotExist
from clean_python.base.domain import Filter
from clean_python.base.domain import Gateway
from clean_python.base.domain import Id
from clean_python.base.domain import Json
from clean_python.base.domain import PageOptions
from clean_python.base.domain import RootEntity

__all__ = ["InternalGateway"]


T = TypeVar("T", bound=RootEntity)  # External


class InternalGateway(Gateway, Generic[T]):
    @abstractproperty
    def manage(self) -> Manage[T]:
        raise NotImplementedError()

    @abstractmethod
    def to_internal(self, obj: T) -> Json:
        raise NotImplementedError()

    def to_external(self, values: Json) -> Json:
        return values

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        page = await self.manage.filter(filters, params)
        return [self.to_internal(x) for x in page.items]

    async def add(self, item: Json) -> Json:
        try:
            created = await self.manage.create(self.to_external(item))
        except BadRequest as e:
            raise ValueError(e)
        return self.to_internal(created)

    async def remove(self, id: Id) -> bool:
        return await self.manage.destroy(id)

    async def count(self, filters: List[Filter]) -> int:
        return await self.manage.count(filters)

    async def exists(self, filters: List[Filter]) -> bool:
        return await self.manage.exists(filters)

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        assert if_unmodified_since is None  # unsupported
        values = self.to_external(item)
        id_ = values.pop("id", None)
        if id_ is None:
            raise DoesNotExist("item", id_)
        try:
            updated = await self.manage.update(id_, values)
        except BadRequest as e:
            raise ValueError(e)
        return self.to_internal(updated)
