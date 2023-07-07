from datetime import datetime
from typing import Optional, Type, TypeVar

from clean_python.base.domain.exceptions import BadRequest
from clean_python.base.infrastructure.now import now
from clean_python.base.domain.value_object import ValueObject

T = TypeVar("T", bound="RootEntity")


class RootEntity(ValueObject):
    id: Optional[int] = None
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
