# (c) Nelen & Schuurmans

from typing import Generic
from typing import Optional
from typing import Sequence
from typing import TypeVar

from pydantic import BaseModel

from .types import Id

__all__ = ["Page", "PageOptions"]

T = TypeVar("T")


class PageOptions(BaseModel):
    limit: int
    offset: int = 0
    order_by: str = "id"
    ascending: bool = True
    cursor: Optional[Id] = None


class Page(BaseModel, Generic[T]):
    total: int
    items: Sequence[T]
    limit: Optional[int] = None
    offset: Optional[int] = None
