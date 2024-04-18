# (c) Nelen & Schuurmans

from enum import Enum
from typing import Any

from pydantic import model_validator

from .types import Id
from .value_object import ValueObject

__all__ = ["Filter", "ComparisonFilter", "ComparisonOperator"]


class Filter(ValueObject):
    field: str
    values: list[Any]

    @classmethod
    def for_id(cls, id: Id) -> "Filter":
        return cls(field="id", values=[id])


class ComparisonOperator(str, Enum):
    LT = "lt"
    LE = "le"
    GE = "ge"
    GT = "gt"
    EQ = "eq"
    NE = "ne"


class ComparisonFilter(Filter):
    operator: ComparisonOperator

    @model_validator(mode="after")
    def verify_no_operator_for_multiple_values(self):
        if len(self.values) != 1:
            raise ValueError("ComparisonFilter needs to have exactly one value")
        return self
