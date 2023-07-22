# (c) Nelen & Schuurmans

from typing import Optional
from typing import Type
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import ValidationError

from .exceptions import BadRequest

__all__ = ["ValueObject"]


T = TypeVar("T", bound="ValueObject")


class ValueObject(BaseModel):
    model_config = ConfigDict(frozen=True)

    def run_validation(self: T) -> T:
        try:
            return self.__class__(**self.model_dump())
        except ValidationError as e:
            raise BadRequest(e)

    @classmethod
    def create(cls: Type[T], **values) -> T:
        try:
            return cls(**values)
        except ValidationError as e:
            raise BadRequest(e)

    def update(self: T, **values) -> T:
        try:
            return self.__class__(**{**self.model_dump(), **values})
        except ValidationError as e:
            raise BadRequest(e)

    def __hash__(self):
        return hash(self.__class__) + hash(tuple(self.__dict__.values()))


K = TypeVar("K", bound="ValueObjectWithId")


class ValueObjectWithId(ValueObject):
    id: Optional[int] = None

    def update(self: K, **values) -> K:
        if "id" in values and self.id is not None and values["id"] != self.id:
            raise ValueError("Cannot change the id")
        return super().update(**values)
