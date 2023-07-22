from pydantic import BaseModel
from pydantic import ConfigDict


class DomainService(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
