# (c) Nelen & Schuurmans
from contextlib import contextmanager
from typing import Iterator
from typing import List
from typing import Optional
from typing import TypeVar

import inject
from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import desc
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.sql import Executable
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.expression import false

from clean_python import ctx
from clean_python import Filter
from clean_python import Id
from clean_python import Json
from clean_python import PageOptions
from clean_python import SyncGateway

from .sql_provider import SyncSQLDatabase
from .sql_provider import SyncSQLProvider

__all__ = ["SyncSQLGateway"]


T = TypeVar("T", bound="SyncSQLGateway")


class SyncSQLGateway(SyncGateway):
    table: Table
    nested: bool
    multitenant: bool

    def __init__(
        self,
        provider_override: Optional[SyncSQLProvider] = None,
        nested: bool = False,
    ):
        self.provider_override = provider_override
        self.nested = nested

    @property
    def provider(self):
        return self.provider_override or inject.instance(SyncSQLDatabase)

    def __init_subclass__(cls, table: Table, multitenant: bool = False) -> None:
        cls.table = table
        if multitenant and not hasattr(table.c, "tenant"):
            raise ValueError("Can't use a multitenant SQLGateway without tenant column")
        cls.multitenant = multitenant
        super().__init_subclass__()

    def rows_to_dict(self, rows: List[Json]) -> List[Json]:
        return rows

    def dict_to_row(self, obj: Json) -> Json:
        known = {c.key for c in self.table.c}
        result = {k: obj[k] for k in obj.keys() if k in known}
        if "id" in result and result["id"] is None:
            del result["id"]
        if self.multitenant:
            result["tenant"] = self.current_tenant
        return result

    @contextmanager
    def transaction(self: T) -> Iterator[T]:
        if self.nested:
            yield self
        else:
            with self.provider.transaction() as provider:
                yield self.__class__(provider, nested=True)

    @property
    def current_tenant(self) -> Optional[int]:
        if not self.multitenant:
            return None
        if ctx.tenant is None:
            raise RuntimeError(f"{self.__class__} requires a tenant in the context")
        return ctx.tenant.id

    def execute(self, query: Executable) -> List[Json]:
        assert self.nested
        return self.rows_to_dict(self.provider.execute(query))

    def _filter_to_sql(self, filter: Filter) -> ColumnElement:
        try:
            column = getattr(self.table.c, filter.field)
        except AttributeError:
            return false()
        if len(filter.values) == 0:
            return false()
        elif len(filter.values) == 1:
            return column == filter.values[0]
        else:
            return column.in_(filter.values)

    def _filters_to_sql(self, filters: List[Filter]) -> ColumnElement:
        qs = [self._filter_to_sql(x) for x in filters]
        if self.multitenant:
            qs.append(self.table.c.tenant == self.current_tenant)
        return and_(*qs)

    def _id_filter_to_sql(self, id: Id) -> ColumnElement:
        return self._filters_to_sql([Filter(field="id", values=[id])])

    def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        query = select(self.table).where(self._filters_to_sql(filters))
        if params is not None:
            sort = asc(params.order_by) if params.ascending else desc(params.order_by)
            query = query.order_by(sort).limit(params.limit).offset(params.offset)
        with self.transaction() as transaction:
            result = transaction.execute(query)
            # transaction.get_related(result)
        return result
