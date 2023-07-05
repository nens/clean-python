from pydantic import BaseModel


class DomainService(BaseModel):
    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True
