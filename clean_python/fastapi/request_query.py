# (c) Nelen & Schuurmans

from inspect import signature
from typing import ClassVar
from typing import Literal
from warnings import warn

from fastapi import Depends
from fastapi import Query
from pydantic import ValidationError

from clean_python import BadRequest
from clean_python import ComparisonFilter
from clean_python import Filter
from clean_python import PageOptions
from clean_python import ValueObject

__all__ = ["RequestQuery"]


class RequestQuery(ValueObject):
    """This class standardizes filtering and pagination for list endpoints.

    Example usage in a Resource:

        @get("/books")
        def list_books(self, q: Annotated[RequestQuery, Query()]):
            return self.manage.filter(q.filters(), q.as_page_options())
    """

    SEPARATOR: ClassVar[str] = "__"
    NON_FILTERS: ClassVar[frozenset[str]] = frozenset({"limit", "offset", "order_by"})

    limit: int = Query(50, ge=1, le=100, description="Page size limit")
    offset: int = Query(0, ge=0, description="Page offset")
    order_by: Literal["id", "-id"] = Query(
        default="id", description="Field to order by"
    )

    def __init_subclass__(cls: type["RequestQuery"]) -> None:
        if hasattr(cls, "order_by") and "enum" in cls.order_by.json_schema_extra:  # type: ignore
            raise ValueError(
                "Specifying order_by options with an enum kwarg is deprecated since "
                "clean-python 0.13. Please use the Literal type instead."
            )
        super().__init_subclass__()

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

    @classmethod
    def depends(cls) -> Depends:
        """DEPRECATED: FastAPI now does directly support pydantic models for query parameters.
        See class docstring for usage.

        Source:

        - https://fastapi.tiangolo.com/tutorial/query-param-models/
        - https://github.com/tiangolo/fastapi/issues/1474
        """
        warn(
            "Deprecated: Pydantic models as query parameters are now supported, see class docstring.",
            DeprecationWarning,
            stacklevel=2,
        )

        def wrapper(*args, **kwargs):
            try:
                signature(wrapper).bind(*args, **kwargs)
                return cls(*args, **kwargs)
            except ValidationError as e:
                raise BadRequest(e, loc=("query",))

        wrapper.__signature__ = signature(cls)  # type: ignore
        return Depends(wrapper)
