# (c) Nelen & Schuurmans
from datetime import datetime
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
    builder: SQLBuilder
    mapper: Mapper = Mapper()

    def __init__(self, provider_override: SyncSQLProvider | None = None):
        self.provider_override = provider_override

    def __init_subclass__(cls, table: Table, multitenant: bool = False) -> None:
        cls.builder = SQLBuilder(table, multitenant)
        super().__init_subclass__()

    @property
    def provider(self):
        return self.provider_override or inject.instance(SyncSQLDatabase)

    def add(self, item: Json) -> Json:
        query = self.builder.insert(self.mapper.to_external(item))
        (row,) = self.provider.execute(query)
        return self.mapper.to_internal(row)

    def update(self, item: Json, if_unmodified_since: datetime | None = None) -> Json:
        id_ = item.get("id")
        if id_ is None:
            raise DoesNotExist("record", id_)
        query = self.builder.update(
            id_, self.mapper.to_external(item), if_unmodified_since
        )
        rows = self.provider.execute(query)
        if not rows:
            if if_unmodified_since is not None:
                if self.exists([Filter.for_id(id_)]):
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
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        query = self.builder.select(filters, params)
        rows = self.provider.execute(query)
        return [self.mapper.to_internal(x) for x in rows]

    def count(self, filters: list[Filter]) -> int:
        (row,) = self.provider.execute(self.builder.count(filters))
        return row["count"]

    def exists(self, filters: list[Filter]) -> bool:
        return len(self.provider.execute(self.builder.exists(filters))) > 0
