# (c) Nelen & Schuurmans

from datetime import datetime
from datetime import timezone
from typing import Optional
from typing import Type
from typing import TypeVar

from .exceptions import BadRequest
from .types import Id
from .value_object import ValueObject

__all__ = ["RootEntity", "now"]


def now():
    # this function is there so that we can mock it in tests
    return datetime.now(timezone.utc)


T = TypeVar("T", bound="RootEntity")


class RootEntity(ValueObject):
    id: Optional[Id] = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def create(cls: Type[T], **values) -> T:
        values.setdefault("created_at", now())
        values.setdefault("updated_at", values["created_at"])
        return super(RootEntity, cls).create(**values)

    def update(self: T, **values) -> T:
        if "id" in values and self.id is not None and values["id"] != self.id:
            raise BadRequest("Cannot change the id of an entity")
        values.setdefault("updated_at", now())
        return super().update(**values)

    def __hash__(self):
        assert self.id is not None
        return hash(self.__class__) + hash(self.id)
