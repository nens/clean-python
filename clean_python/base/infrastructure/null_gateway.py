from datetime import datetime
from typing import Callable
from typing import List
from typing import Optional

from clean_python.base.domain import DoesNotExist
from clean_python.base.domain import Filter
from clean_python.base.domain import Gateway
from clean_python.base.domain import Id
from clean_python.base.domain import Json
from clean_python.base.domain import PageOptions

__all__ = ["NullGateway"]


class NullGateway(Gateway):
    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        return []

    async def add(self, item: Json) -> Json:
        return item

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        raise DoesNotExist("record", item.get("id"))

    async def update_transactional(self, id: Id, func: Callable[[Json], Json]) -> Json:
        raise DoesNotExist("record", id)

    async def upsert(self, item: Json) -> Json:
        return item

    async def remove(self, id: Id) -> bool:
        return False
