# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .exceptions import AlreadyExists, Conflict, DoesNotExist
from .pagination import PageOptions
from .value_object import ValueObject

__all__ = ["Gateway", "Json", "Filter", "InMemoryGateway"]
Json = Dict[str, Any]


class Filter(ValueObject):
    field: str
    values: List[Any]


class Gateway:
    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        raise NotImplementedError()

    async def count(self, filters: List[Filter]) -> int:
        return len(await self.filter(filters, params=None))

    async def exists(self, filters: List[Filter]) -> bool:
        return len(await self.filter(filters, params=PageOptions(limit=1))) > 0

    async def get(self, id: int) -> Optional[Json]:
        result = await self.filter([Filter(field="id", values=[id])], params=None)
        return result[0] if result else None

    async def add(self, item: Json) -> Json:
        raise NotImplementedError()

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        raise NotImplementedError()

    async def update_transactional(self, id: int, func: Callable[[Json], Json]) -> Json:
        existing = await self.get(id)
        if existing is None:
            raise DoesNotExist("record", id)
        return await self.update(
            func(existing), if_unmodified_since=existing["updated_at"]
        )

    async def upsert(self, item: Json) -> Json:
        try:
            return await self.update(item)
        except DoesNotExist:
            return await self.add(item)

    async def remove(self, id: int) -> bool:
        raise NotImplementedError()


class InMemoryGateway(Gateway):
    """For testing purposes"""

    def __init__(self, data: List[Json]):
        self.data = {x["id"]: deepcopy(x) for x in data}

    def _get_next_id(self) -> int:
        if len(self.data) == 0:
            return 1
        else:
            return max(self.data) + 1

    def _paginate(self, objs: List[Json], params: PageOptions) -> List[Json]:
        objs = sorted(
            objs,
            key=lambda x: (x.get(params.order_by) is None, x.get(params.order_by)),
            reverse=not params.ascending,
        )
        return objs[params.offset : params.offset + params.limit]

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        result = []
        for x in self.data.values():
            for filter in filters:
                if x.get(filter.field) not in filter.values:
                    break
            else:
                result.append(deepcopy(x))
        if params is not None:
            result = self._paginate(result, params)
        return result

    async def add(self, item: Json) -> Json:
        item = item.copy()
        id_ = item.pop("id", None)
        # autoincrement (like SQL does)
        if id_ is None:
            id_ = self._get_next_id()
        elif id_ in self.data:
            raise AlreadyExists(id_)

        self.data[id_] = {"id": id_, **item}
        return deepcopy(self.data[id_])

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        _id = item.get("id")
        if _id is None or _id not in self.data:
            raise DoesNotExist("item", _id)
        existing = self.data[_id]
        if if_unmodified_since and existing.get("updated_at") != if_unmodified_since:
            raise Conflict()
        existing.update(item)
        return deepcopy(existing)

    async def remove(self, id: int) -> bool:
        if id not in self.data:
            return False
        del self.data[id]
        return True
