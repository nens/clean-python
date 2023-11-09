# (c) Nelen & Schuurmans

from typing import List

from fastapi import Query
from pydantic import field_validator

from clean_python import Filter
from clean_python import PageOptions
from clean_python import ValueObject

__all__ = ["RequestQuery"]


class RequestQuery(ValueObject):
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

    def filters(self) -> List[Filter]:
        result = []
        for name in self.model_fields:
            if name in {"limit", "offset", "order_by"}:
                continue
            # deal with list query paramerers
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, list):
                    value = [value]
                result.append(Filter(field=name, values=value))
        return result
