# (c) Nelen & Schuurmans

from copy import deepcopy
from datetime import datetime
from typing import List
from typing import Optional

from clean_python.base.domain import AlreadyExists
from clean_python.base.domain import Conflict
from clean_python.base.domain import DoesNotExist
from clean_python.base.domain import Filter
from clean_python.base.domain import Gateway
from clean_python.base.domain import Json
from clean_python.base.domain import PageOptions

__all__ = ["InMemoryGateway"]


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
