from typing import FrozenSet
from typing import Optional

from clean_python import ValueObject

__all__ = ["Claims", "User", "Tenant"]


class Tenant(ValueObject):
    id: int
    name: str


class User(ValueObject):
    id: str
    name: Optional[str]


class Claims(ValueObject):
    user: User
    tenant: Optional[Tenant]
    scope: FrozenSet[str]
