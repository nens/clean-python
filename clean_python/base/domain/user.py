from typing import FrozenSet

from .value_object import ValueObject

__all__ = ["User", "Tenant", "Scope"]


class User(ValueObject):
    id: str
    name: str


Scope = FrozenSet[str]


class Tenant(ValueObject):
    id: int
    name: str
