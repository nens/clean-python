# (c) Nelen & Schuurmans
from datetime import datetime
from typing import List
from typing import Optional
from typing import TypeVar

import inject
from sqlalchemy import Table

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Id
from clean_python import Json
from clean_python import Mapper
from clean_python import PageOptions
from clean_python import SyncGateway

from .sql_builder import SQLBuilder
from .sql_provider import SyncSQLDatabase
from .sql_provider import SyncSQLProvider

__all__ = ["SyncSQLGateway"]


T = TypeVar("T", bound="SyncSQLGateway")


class SyncSQLGateway(SyncGateway):
    table: Table | None = None
    multitenant: bool = False
    mapper: Mapper = Mapper()

    def __init__(self, provider_override: Optional[SyncSQLProvider] = None):
        assert self.table is not None
        if self.multitenant and not hasattr(self.table.c, "tenant"):
            raise ValueError(
                "Can't use a multitenant SyncSQLGateway without tenant column"
            )
        self.provider_override = provider_override
        self.builder = SQLBuilder(self.table, self.multitenant)

    @property
    def provider(self):
        return self.provider_override or inject.instance(SyncSQLDatabase)

    def add(self, item: Json) -> Json:
        query = self.builder.insert(self.mapper.to_external(item))
        (row,) = self.provider.execute(query)
        return self.mapper.to_internal(row)

    def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        id_ = item.get("id")
        if id_ is None:
            raise DoesNotExist("record", id_)
        query = self.builder.update(
            id_, self.mapper.to_external(item), if_unmodified_since
        )
        rows = self.provider.execute(query)
        if not rows:
            if if_unmodified_since is not None:
                if self.exists([Filter(field="id", values=[id_])]):
                    raise Conflict()
            raise DoesNotExist("record", id_)
        assert len(rows) == 1
        return self.mapper.to_internal(rows[0])

    def upsert(self, item: Json) -> Json:
        if item.get("id") is None:
            return self.add(item)
        query = self.builder.upsert(self.mapper.to_external(item))
        (row,) = self.provider.execute(query)
        return self.mapper.to_internal(row)

    def remove(self, id: Id) -> bool:
        return bool(self.provider.execute(self.builder.delete(id)))

    def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        query = self.builder.select(filters, params)
        rows = self.provider.execute(query)
        return [self.mapper.to_internal(x) for x in rows]

    def count(self, filters: List[Filter]) -> int:
        (row,) = self.provider.execute(self.builder.count(filters))
        return row["count"]

    def exists(self, filters: List[Filter]) -> bool:
        return len(self.provider.execute(self.builder.exists(filters))) > 0
