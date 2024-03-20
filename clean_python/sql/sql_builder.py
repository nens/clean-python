from datetime import datetime

from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import delete
from sqlalchemy import desc
from sqlalchemy import Executable
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import true
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.expression import false

from clean_python import ctx
from clean_python import Filter
from clean_python import Id
from clean_python import Json
from clean_python import PageOptions

__all__ = ["SQLBuilder"]


class SQLBuilder:
    def __init__(self, table: Table, multitenant: bool = False):
        if multitenant and not hasattr(table.c, "tenant"):
            raise ValueError("Can't use a multitenant SQLBuilder without tenant column")
        self.table = table
        self.multitenant = multitenant

    @property
    def current_tenant(self) -> Id | None:
        if not self.multitenant:
            return None
        if ctx.tenant is None:
            raise RuntimeError(f"{self.__class__} requires a tenant in the context")
        return ctx.tenant.id

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

    def _filters_to_sql(self, filters: list[Filter]) -> ColumnElement:
        qs = [self._filter_to_sql(x) for x in filters]
        if self.multitenant:
            qs.append(self.table.c.tenant == self.current_tenant)
        return and_(*qs)

    def _id_filter_to_sql(self, id: Id) -> ColumnElement:
        return self._filters_to_sql([Filter(field="id", values=[id])])

    def _santize_item(self, item: Json) -> Json:
        known = {c.key for c in self.table.c}
        result = {k: item[k] for k in item.keys() if k in known}
        if "id" in result and result["id"] is None:
            del result["id"]
        if self.multitenant:
            result["tenant"] = self.current_tenant
        return result

    def select(
        self,
        filters: list[Filter],
        params: PageOptions | None = None,
        for_update: bool = False,
    ) -> Executable:
        query = select(self.table)
        if for_update:
            query = query.with_for_update()
        query = query.where(self._filters_to_sql(filters))
        if params is not None:
            sort = asc(params.order_by) if params.ascending else desc(params.order_by)
            query = query.order_by(sort).limit(params.limit).offset(params.offset)
        return query

    def insert(self, item: Json) -> Executable:
        return (
            insert(self.table).values(**self._santize_item(item)).returning(self.table)
        )

    def upsert(self, item: Json) -> Executable:
        item = self._santize_item(item)
        return (
            insert(self.table)
            .values(**item)
            .on_conflict_do_update(
                index_elements=["id", "tenant"] if self.multitenant else ["id"],
                set_=item,
            )
            .returning(self.table)
        )

    def update(self, id: Id, item: Json, if_unmodified_since: datetime | None):
        q = self._id_filter_to_sql(id)
        if if_unmodified_since is not None:
            q &= self.table.c.updated_at == if_unmodified_since
        return (
            update(self.table)
            .where(q)
            .values(**self._santize_item(item))
            .returning(self.table)
        )

    def delete(self, id: Id) -> Executable:
        return (
            delete(self.table)
            .where(self._id_filter_to_sql(id))
            .returning(self.table.c.id)
        )

    def count(self, filters: list[Filter]) -> Executable:
        return (
            select(func.count().label("count"))
            .select_from(self.table)
            .where(self._filters_to_sql(filters))
        )

    def exists(self, filters: list[Filter]) -> Executable:
        return (
            select(true().label("exists"))
            .select_from(self.table)
            .where(self._filters_to_sql(filters))
            .limit(1)
        )
