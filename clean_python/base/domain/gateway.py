# (c) Nelen & Schuurmans

from abc import ABC
from datetime import datetime
from typing import Callable
from typing import List
from typing import Optional

from .exceptions import DoesNotExist
from .filter import Filter
from .json import Json
from .pagination import PageOptions

__all__ = ["Gateway"]


class Gateway(ABC):
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
