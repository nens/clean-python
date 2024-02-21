# (c) Nelen & Schuurmans

from typing import Any

from .types import Id
from .value_object import ValueObject

__all__ = ["Filter"]


class Filter(ValueObject):
    field: str
    values: list[Any]

    @classmethod
    def for_id(cls, id: Id) -> "Filter":
        return cls(field="id", values=[id])
