# (c) Nelen & Schuurmans

from typing import Any
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import Union

from .exceptions import DoesNotExist
from .filter import Filter
from .gateway import Gateway
from .gateway import SyncGateway
from .pagination import Page
from .pagination import PageOptions
from .types import Id
from .types import Json
from .value_object import ValueObject

__all__ = ["Repository", "SyncRepository"]

T = TypeVar("T", bound=ValueObject)


class Repository(Generic[T]):
    entity: Type[T]

    def __init__(self, gateway: Gateway):
        self.gateway = gateway

    def __init_subclass__(cls) -> None:
        (base,) = cls.__orig_bases__  # type: ignore
        (entity,) = base.__args__
        super().__init_subclass__()
        cls.entity = entity

    async def all(self, params: Optional[PageOptions] = None) -> Page[T]:
        return await self.filter([], params=params)

    async def by(
        self, key: str, value: Any, params: Optional[PageOptions] = None
    ) -> Page[T]:
        return await self.filter([Filter(field=key, values=[value])], params=params)

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> Page[T]:
        records = await self.gateway.filter(filters, params=params)
        total = len(records)
        # when using pagination, we may need to do a count in the db
        # except in a typical 'first page' situation with few records
        if params is not None and not (params.offset == 0 and total < params.limit):
            total = await self.count(filters)
        return Page(
            total=total,
            limit=params.limit if params else None,
            offset=params.offset if params else None,
            items=[self.entity(**x) for x in records],
        )

    async def get(self, id: Id) -> T:
        res = await self.gateway.get(id)
        if res is None:
            raise DoesNotExist("object", id)
        else:
            return self.entity(**res)

    async def add(self, item: Union[T, Json]) -> T:
        if isinstance(item, dict):
            item = self.entity.create(**item)
        created = await self.gateway.add(item.model_dump())
        return self.entity(**created)

    async def update(self, id: Id, values: Json) -> T:
        if not values:
            return await self.get(id)
        updated = await self.gateway.update_transactional(
            id, lambda x: self.entity(**x).update(**values).model_dump()
        )
        return self.entity(**updated)

    async def upsert(self, item: T) -> T:
        values = item.model_dump()
        upserted = await self.gateway.upsert(values)
        return self.entity(**upserted)

    async def remove(self, id: Id) -> bool:
        return await self.gateway.remove(id)

    async def count(self, filters: List[Filter]) -> int:
        return await self.gateway.count(filters)

    async def exists(self, filters: List[Filter]) -> bool:
        return await self.gateway.exists(filters)


# This is a copy-paste from Repository, but with all the async / await removed


class SyncRepository(Generic[T]):
    entity: Type[T]

    def __init__(self, gateway: SyncGateway):
        self.gateway = gateway

    def __init_subclass__(cls) -> None:
        (base,) = cls.__orig_bases__  # type: ignore
        (entity,) = base.__args__
        super().__init_subclass__()
        cls.entity = entity

    def all(self, params: Optional[PageOptions] = None) -> Page[T]:
        return self.filter([], params=params)

    def by(self, key: str, value: Any, params: Optional[PageOptions] = None) -> Page[T]:
        return self.filter([Filter(field=key, values=[value])], params=params)

    def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> Page[T]:
        records = self.gateway.filter(filters, params=params)
        total = len(records)
        # when using pagination, we may need to do a count in the db
        # except in a typical 'first page' situation with few records
        if params is not None and not (params.offset == 0 and total < params.limit):
            total = self.count(filters)
        return Page(
            total=total,
            limit=params.limit if params else None,
            offset=params.offset if params else None,
            items=[self.entity(**x) for x in records],
        )

    def get(self, id: Id) -> T:
        res = self.gateway.get(id)
        if res is None:
            raise DoesNotExist("object", id)
        else:
            return self.entity(**res)

    def add(self, item: Union[T, Json]) -> T:
        if isinstance(item, dict):
            item = self.entity.create(**item)
        created = self.gateway.add(item.model_dump())
        return self.entity(**created)

    def update(self, id: Id, values: Json) -> T:
        if not values:
            return self.get(id)
        updated = self.gateway.update_transactional(
            id, lambda x: self.entity(**x).update(**values).model_dump()
        )
        return self.entity(**updated)

    def upsert(self, item: T) -> T:
        values = item.model_dump()
        upserted = self.gateway.upsert(values)
        return self.entity(**upserted)

    def remove(self, id: Id) -> bool:
        return self.gateway.remove(id)

    def count(self, filters: List[Filter]) -> int:
        return self.gateway.count(filters)

    def exists(self, filters: List[Filter]) -> bool:
        return self.gateway.exists(filters)
