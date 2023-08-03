from typing import FrozenSet
from typing import Optional

from clean_python import ValueObject

__all__ = ["Claims"]


class Tenant(ValueObject):
    id: int
    name: str


class Claims(ValueObject):
    scope: FrozenSet[str]
    tenant: Optional[Tenant]
