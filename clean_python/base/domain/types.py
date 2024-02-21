# (c) Nelen & Schuurmans

from typing import Any
from typing import Union
from uuid import UUID

__all__ = ["Json", "Id"]


Json = dict[str, Any]
Id = Union[int, str, UUID]
