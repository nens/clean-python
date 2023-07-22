# (c) Nelen & Schuurmans

from pydantic import BaseModel
from pydantic import ConfigDict

__all__ = ["DomainService"]


class DomainService(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
