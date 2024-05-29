# (c) Nelen & Schuurmans
from abc import abstractmethod
from datetime import datetime
from typing import Generic
from typing import TypeVar

from clean_python.base.application.manage import Manage
from clean_python.base.application.manage import SyncManage
from clean_python.base.domain import BadRequest
from clean_python.base.domain import DoesNotExist
from clean_python.base.domain import Filter
from clean_python.base.domain import Gateway
from clean_python.base.domain import Id
from clean_python.base.domain import Json
from clean_python.base.domain import PageOptions
from clean_python.base.domain import RootEntity
from clean_python.base.domain import SyncGateway

from .mapper import Mapper

__all__ = ["InternalGateway", "SyncInternalGateway"]


T = TypeVar("T", bound=RootEntity)  # External


class InternalGateway(Gateway, Generic[T]):
    mapper: Mapper

    @property
    @abstractmethod
    def manage(self) -> Manage[T]:
        raise NotImplementedError()

    async def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        page = await self.manage.filter(filters, params)
        return [self.mapper.to_internal(x) for x in page.items]

    async def add(self, item: Json) -> Json:
        try:
            created = await self.manage.create(self.mapper.to_external(item))
        except BadRequest as e:
            raise ValueError(e)
        return self.mapper.to_internal(created)

    async def remove(self, id: Id) -> bool:
        return await self.manage.destroy(id)

    async def count(self, filters: list[Filter]) -> int:
        return await self.manage.count(filters)

    async def exists(self, filters: list[Filter]) -> bool:
        return await self.manage.exists(filters)

    async def update(
        self, item: Json, if_unmodified_since: datetime | None = None
    ) -> Json:
        assert if_unmodified_since is None  # unsupported
        values = self.mapper.to_external(item)
        id_ = values.pop("id", None)
        if id_ is None:
            raise DoesNotExist("item", id_)
        try:
            updated = await self.manage.update(id_, values)
        except BadRequest as e:
            raise ValueError(e)
        return self.mapper.to_internal(updated)


class SyncInternalGateway(SyncGateway, Generic[T]):
    mapper: Mapper

    @property
    @abstractmethod
    def manage(self) -> SyncManage[T]:
        raise NotImplementedError()

    def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        page = self.manage.filter(filters, params)
        return [self.mapper.to_internal(x) for x in page.items]

    def add(self, item: Json) -> Json:
        try:
            created = self.manage.create(self.mapper.to_external(item))
        except BadRequest as e:
            raise ValueError(e)
        return self.mapper.to_internal(created)

    def remove(self, id: Id) -> bool:
        return self.manage.destroy(id)

    def count(self, filters: list[Filter]) -> int:
        return self.manage.count(filters)

    def exists(self, filters: list[Filter]) -> bool:
        return self.manage.exists(filters)

    def update(self, item: Json, if_unmodified_since: datetime | None = None) -> Json:
        assert if_unmodified_since is None  # unsupported
        values = self.mapper.to_external(item)
        id_ = values.pop("id", None)
        if id_ is None:
            raise DoesNotExist("item", id_)
        try:
            updated = self.manage.update(id_, values)
        except BadRequest as e:
            raise ValueError(e)
        return self.mapper.to_internal(updated)
