# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans
from abc import abstractmethod, abstractproperty
from typing import Generic, List, Optional, TypeVar

from clean_python.base.domain.exceptions import BadRequest, DoesNotExist
from clean_python.base.infrastructure.gateway import Filter
from clean_python.base.application.manage import Manage
from clean_python.base.domain.pagination import PageOptions
from clean_python.base.domain.root_entity import RootEntity
from clean_python.base.domain.value_object import ValueObject

E = TypeVar("E", bound=RootEntity)  # External
T = TypeVar("T", bound=ValueObject)  # Internal


# don't subclass Gateway; Gateway makes Json objects
class InternalGateway(Generic[E, T]):
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
            created = await self.manage.create(item.dict())
        except BadRequest as e:
            raise ValueError(e)
        return self._map(created)

    async def remove(self, id) -> bool:
        return await self.manage.destroy(id)

    async def count(self, filters: List[Filter]) -> int:
        return await self.manage.count(filters)

    async def exists(self, filters: List[Filter]) -> bool:
        return await self.manage.exists(filters)
