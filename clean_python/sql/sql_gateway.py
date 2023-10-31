# (c) Nelen & Schuurmans
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator
from typing import Callable
from typing import List
from typing import Optional
from typing import TypeVar

import inject
from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import delete
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import true
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import Executable
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.expression import false

from clean_python import Conflict
from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Gateway
from clean_python import Id
from clean_python import Json
from clean_python import PageOptions

from .sql_provider import SQLDatabase
from .sql_provider import SQLProvider

__all__ = ["SQLGateway"]


T = TypeVar("T", bound="SQLGateway")


class SQLGateway(Gateway):
    table: Table
    nested: bool
    multitenant: bool

    def __init__(
        self,
        provider_override: Optional[SQLProvider] = None,
        nested: bool = False,
    ):
        self.provider_override = provider_override
        self.nested = nested

    @property
    def provider(self):
        return self.provider_override or inject.instance(SQLDatabase)

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

    @asynccontextmanager
    async def transaction(self: T) -> AsyncIterator[T]:
        if self.nested:
            yield self
        else:
            async with self.provider.transaction() as provider:
                yield self.__class__(provider, nested=True)

    @property
    def current_tenant(self) -> Optional[int]:
        if not self.multitenant:
            return None
        if ctx.tenant is None:
            raise RuntimeError(f"{self.__class__} requires a tenant in the context")
        return ctx.tenant.id

    async def get_related(self, items: List[Json]) -> None:
        pass

    async def set_related(self, item: Json, result: Json) -> None:
        pass

    async def execute(self, query: Executable) -> List[Json]:
        assert self.nested
        return self.rows_to_dict(await self.provider.execute(query))

    async def add(self, item: Json) -> Json:
        query = (
            insert(self.table).values(**self.dict_to_row(item)).returning(self.table)
        )
        async with self.transaction() as transaction:
            (result,) = await transaction.execute(query)
            await transaction.set_related(item, result)
        return result

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        id_ = item.get("id")
        if id_ is None:
            raise DoesNotExist("record", id_)
        q = self._id_filter_to_sql(id_)
        if if_unmodified_since is not None:
            q &= self.table.c.updated_at == if_unmodified_since
        query = (
            update(self.table)
            .where(q)
            .values(**self.dict_to_row(item))
            .returning(self.table)
        )
        async with self.transaction() as transaction:
            result = await transaction.execute(query)
            if not result:
                if if_unmodified_since is not None:
                    # note: the get() is to maybe raise DoesNotExist
                    if await self.get(id_):
                        raise Conflict()
                raise DoesNotExist("record", id_)
            await transaction.set_related(item, result[0])
        return result[0]

    async def _select_for_update(self, id: Id) -> Json:
        async with self.transaction() as transaction:
            result = await transaction.execute(
                select(self.table).with_for_update().where(self._id_filter_to_sql(id)),
            )
            if not result:
                raise DoesNotExist("record", id)
            await transaction.get_related(result)
        return result[0]

    async def update_transactional(self, id: Id, func: Callable[[Json], Json]) -> Json:
        async with self.transaction() as transaction:
            existing = await transaction._select_for_update(id)
            updated = func(existing)
            return await transaction.update(updated)

    async def upsert(self, item: Json) -> Json:
        if item.get("id") is None:
            return await self.add(item)
        values = self.dict_to_row(item)
        query = (
            insert(self.table)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["id", "tenant"] if self.multitenant else ["id"],
                set_=values,
            )
            .returning(self.table)
        )
        async with self.transaction() as transaction:
            result = await transaction.execute(query)
            await transaction.set_related(item, result[0])
        return result[0]

    async def remove(self, id: Id) -> bool:
        query = (
            delete(self.table)
            .where(self._id_filter_to_sql(id))
            .returning(self.table.c.id)
        )
        async with self.transaction() as transaction:
            result = await transaction.execute(query)
        return bool(result)

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

    async def filter(
        self, filters: List[Filter], params: Optional[PageOptions] = None
    ) -> List[Json]:
        query = select(self.table).where(self._filters_to_sql(filters))
        if params is not None:
            sort = asc(params.order_by) if params.ascending else desc(params.order_by)
            query = query.order_by(sort).limit(params.limit).offset(params.offset)
        async with self.transaction() as transaction:
            result = await transaction.execute(query)
            await transaction.get_related(result)
        return result

    async def count(self, filters: List[Filter]) -> int:
        query = (
            select(func.count().label("count"))
            .select_from(self.table)
            .where(self._filters_to_sql(filters))
        )
        async with self.transaction() as transaction:
            return (await transaction.execute(query))[0]["count"]

    async def exists(self, filters: List[Filter]) -> bool:
        query = (
            select(true().label("exists"))
            .select_from(self.table)
            .where(self._filters_to_sql(filters))
            .limit(1)
        )
        async with self.transaction() as transaction:
            return len(await transaction.execute(query)) > 0

    async def _get_related_one_to_many(
        self,
        items: List[Json],
        field_name: str,
        fk_name: str,
    ) -> None:
        """Fetch related objects for `items` and add them inplace.

        The result is `items` having an additional field containing a list of related
        objects which were retrieved from self in 1 SELECT query.

        Args:
            items: The items for which to fetch related objects. Changed inplace.
            field_name: The key in item to put the fetched related objects into.
            fk_name: The column name on the related object that refers to item["id"]

        Example:
            Writer has a one-to-many relation to books.

            >>> writers = [{"id": 2, "name": "John Doe"}]
            >>> _get_related_one_to_many(
                items=writers,
                related_gateway=BookSQLGateway,
                field_name="books",
                fk_name="writer_id",
            )
            >>> writers[0]
            {
                "id": 2,
                "name": "John Doe",
                "books": [
                    {
                        "id": 1",
                        "title": "How to write an ORM",
                        "writer_id": 2
                    }
                ]
            }
        """
        assert not self.multitenant
        for x in items:
            x[field_name] = []
        item_lut = {x["id"]: x for x in items}
        related_objs = await self.filter(
            [Filter(field=fk_name, values=list(item_lut.keys()))]
        )
        for related_obj in related_objs:
            item_lut[related_obj[fk_name]][field_name].append(related_obj)

    async def _set_related_one_to_many(
        self,
        item: Json,
        result: Json,
        field_name: str,
        fk_name: str,
    ) -> None:
        """Set related objects for `item`

        This method first fetches the current situation and then adds / updates / removes
        where appropriate.

        Args:
            item: The item for which to set related objects.
            result: The dictionary to put the resulting (added / updated) objects into
            field_name: The key in result to put the (added / updated) related objects into.
            fk_name: The column name on the related object that refers to item["id"]

        Example:
            Writer has a one-to-many relation to books.

            >>> writer = {"id": 2, "name": "John Doe", "books": {"title": "Foo"}}
            >>> _set_related_one_to_many(
                item=writer,
                result=writer,
                related_gateway=BookSQLGateway,
                field_name="books",
                fk_name="writer_id",
            )
            >>> result
            {
                "id": 2,
                "name": "John Doe",
                "books": [
                    {
                        "id": 1",
                        "title": "Foo",
                        "writer_id": 2
                    }
                ]
            }
        """
        assert not self.multitenant
        # list existing related objects
        existing_lut = {
            x["id"]: x
            for x in await self.filter([Filter(field=fk_name, values=[result["id"]])])
        }

        # add / update them where necessary
        returned = []
        for new_value in item.get(field_name, []):
            new_value = {fk_name: result["id"], **new_value}
            existing = existing_lut.pop(new_value.get("id"), None)
            if existing is None:
                returned.append(await self.add(new_value))
            elif new_value == existing:
                returned.append(existing)
            else:
                returned.append(await self.update(new_value))

        result[field_name] = returned

        # remove remaining
        for to_remove in existing_lut:
            assert await self.remove(to_remove)
