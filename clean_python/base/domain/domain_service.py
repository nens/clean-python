# (c) Nelen & Schuurmans

from pydantic import BaseModel

__all__ = ["DomainService"]


class DomainService(BaseModel):
    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True
