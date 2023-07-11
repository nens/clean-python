# (c) Nelen & Schuurmans

from typing import Generic
from typing import Optional
from typing import Sequence
from typing import TypeVar

from pydantic import BaseModel
from pydantic.generics import GenericModel

__all__ = ["Page", "PageOptions"]

T = TypeVar("T")


class PageOptions(BaseModel):
    limit: int
    offset: int = 0
    order_by: str = "id"
    ascending: bool = True


class Page(GenericModel, Generic[T]):
    total: int
    items: Sequence[T]
    limit: Optional[int] = None
    offset: Optional[int] = None
