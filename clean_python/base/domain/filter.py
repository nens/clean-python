# (c) Nelen & Schuurmans

from typing import Any
from typing import List

from .value_object import ValueObject

__all__ = ["Filter"]


class Filter(ValueObject):
    field: str
    values: List[Any]
