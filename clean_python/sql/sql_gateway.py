# (c) Nelen & Schuurmans
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TypeVar

import inject
from sqlalchemy import Table
from sqlalchemy.sql import Executable

from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Gateway
from clean_python import Id
from clean_python import Json
from clean_python import Mapper
from clean_python import PageOptions

from .sql_builder import SQLBuilder
from .sql_provider import SQLDatabase
from .sql_provider import SQLProvider

__all__ = ["SQLGateway"]


T = TypeVar("T", bound="SQLGateway")


class SQLGateway(Gateway):
    table: Table
    multitenant: bool
    has_related: bool
    mapper: Mapper = Mapper()

    def __init__(
        self,
        provider_override: SQLProvider | None = None,
        nested: bool = False,
    ):
        self.provider_override = provider_override
        self.nested = nested
        self.builder = SQLBuilder(self.table, self.multitenant)

    @property
    def provider(self):
        return self.provider_override or inject.instance(SQLDatabase)

    def __init_subclass__(
        cls, table: Table, multitenant: bool = False, has_related: bool = False
    ) -> None:
        cls.table = table
        if multitenant and not hasattr(table.c, "tenant"):
            raise ValueError("Can't use a multitenant SQLGateway without tenant column")
        cls.multitenant = multitenant
        cls.has_related = has_related
        super().__init_subclass__()

    @asynccontextmanager
    async def transaction(self: T) -> AsyncIterator[T]:
        if self.nested:
            yield self
        else:
            async with self.provider.transaction() as provider:
                yield self.__class__(provider, nested=True)

    async def get_related(self, items: list[Json]) -> None:
        """Implement this to use transactions for consistently getting nested records"""

    async def set_related(self, item: Json, result: Json) -> None:
        """Implement this to use transactions for consistently setting nested records"""

    async def execute(self, query: Executable) -> list[Json]:
        return [self.mapper.to_internal(x) for x in await self.provider.execute(query)]

    async def add(self, item: Json) -> Json:
        query = self.builder.insert(self.mapper.to_external(item))
        if self.has_related:
            async with self.transaction() as transaction:
                (result,) = await transaction.execute(query)
                await transaction.set_related(item, result)
        else:
            (result,) = await self.execute(query)
        return result

    async def update(
        self, item: Json, if_unmodified_since: datetime | None = None
    ) -> Json:
        id_ = item.get("id")
        if id_ is None:
            raise DoesNotExist("record", id_)
        query = self.builder.update(
            id_, self.mapper.to_external(item), if_unmodified_since
        )
        if self.has_related:
            async with self.transaction() as transaction:
                result = await transaction.execute(query)
                if result:
                    await transaction.set_related(item, result[0])
        else:
            result = await self.execute(query)
        if not result:
            if if_unmodified_since is not None:
                if await self.exists([Filter.for_id(id_)]):
                    raise Conflict()
            raise DoesNotExist("record", id_)
        return result[0]

    async def _select_for_update(self, id: Id) -> Json:
        query = self.builder.select([Filter.for_id(id)], for_update=True)
        async with self.transaction() as transaction:
            result = await transaction.execute(query)
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
        query = self.builder.upsert(self.mapper.to_external(item))
        if self.has_related:
            async with self.transaction() as transaction:
                result = await transaction.execute(query)
                await transaction.set_related(item, result[0])
        else:
            result = await self.execute(query)
        return result[0]

    async def remove(self, id: Id) -> bool:
        return bool(await self.execute(self.builder.delete(id)))

    async def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        query = self.builder.select(filters, params)
        if self.has_related:
            async with self.transaction() as transaction:
                result = await transaction.execute(query)
                await transaction.get_related(result)
        else:
            result = await self.execute(query)
        return result

    async def count(self, filters: list[Filter]) -> int:
        return (await self.execute(self.builder.count(filters)))[0]["count"]

    async def exists(self, filters: list[Filter]) -> bool:
        return len(await self.execute(self.builder.exists(filters))) > 0

    async def _get_related_one_to_many(
        self,
        items: list[Json],
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
