# (c) Nelen & Schuurmans

from typing import ClassVar

from fastapi import Query
from pydantic import field_validator

from clean_python import ComparisonFilter
from clean_python import Filter
from clean_python import PageOptions
from clean_python import ValueObject

__all__ = ["RequestQuery"]


class RequestQuery(ValueObject):
    SEPARATOR: ClassVar[str] = "__"
    NON_FILTERS: ClassVar[frozenset[str]] = frozenset({"limit", "offset", "order_by"})

    limit: int = Query(50, ge=1, le=100, description="Page size limit")
    offset: int = Query(0, ge=0, description="Page offset")
    order_by: str = Query(
        default="id", enum=["id", "-id"], description="Field to order by"
    )

    @field_validator("order_by")
    def validate_order_by_enum(cls, v, _):
        # the 'enum' parameter doesn't actually do anthing in validation
        # See: https://github.com/tiangolo/fastapi/issues/2910
        allowed = cls.model_json_schema()["properties"]["order_by"]["enum"]
        if v not in allowed:
            raise ValueError(f"'order_by' must be one of {allowed}")
        return v

    def as_page_options(self) -> PageOptions:
        if self.order_by.startswith("-"):
            order_by = self.order_by[1:]
            ascending = False
        else:
            order_by = self.order_by
            ascending = True
        return PageOptions(
            limit=self.limit, offset=self.offset, order_by=order_by, ascending=ascending
        )

    def _regular_filter(self, name, value) -> Filter:
        # deal with list query paramerers
        if not isinstance(value, list):
            value = [value]
        return Filter(field=name, values=value)

    def _comparison_filter(self, name, value) -> ComparisonFilter:
        field, operator = name.rsplit(self.SEPARATOR, 1)
        return ComparisonFilter(
            field=field,
            values=[value],
            operator=operator,
        )

    def filters(self) -> list[Filter]:
        result: list[Filter] = []
        for name in self.model_fields:
            if name in self.NON_FILTERS:
                continue
            value = getattr(self, name)
            if value is None:
                continue
            if self.SEPARATOR in name:
                result.append(self._comparison_filter(name, value))
            else:
                result.append(self._regular_filter(name, value))
        return result
