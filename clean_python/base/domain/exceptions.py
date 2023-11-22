# (c) Nelen & Schuurmans

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from pydantic import create_model
from pydantic import ValidationError

from .types import Id

__all__ = [
    "AlreadyExists",
    "Conflict",
    "DoesNotExist",
    "PermissionDenied",
    "PreconditionFailed",
    "BadRequest",
    "Unauthorized",
    "BadRequest",
]


class DoesNotExist(Exception):
    def __init__(self, name: str, id: Optional[Id] = None):
        super().__init__()
        self.name = name
        self.id = id

    def __str__(self):
        if self.id:
            return f"does not exist: {self.name} with id={self.id}"
        else:
            return f"does not exist: {self.name}"


class Conflict(Exception):
    def __init__(self, msg: Optional[str] = None):
        super().__init__(msg)


class AlreadyExists(Exception):
    def __init__(self, value: Any = None, key: str = "id"):
        super().__init__(f"record with {key}={value} already exists")


class PreconditionFailed(Exception):
    def __init__(self, msg: str = "precondition failed", obj: Any = None):
        super().__init__(msg)
        self.obj = obj


# pydantic.ValidationError needs some model; for us it doesn't matter
# We do it the same way as FastAPI does it.
request_model = create_model("Request")


class BadRequest(Exception):
    def __init__(self, err_or_msg: Union[ValidationError, str]):
        self._internal_error = err_or_msg
        super().__init__(err_or_msg)

    def errors(self) -> List[Dict[str, Any]]:
        if isinstance(self._internal_error, ValidationError):
            return [dict() for x in self._internal_error.errors()]
        return [{"error": self}]

    def __str__(self) -> str:
        error = self._internal_error
        if isinstance(error, ValidationError):
            details = error.errors()[0]
            loc = "'" + ",".join([str(x) for x in details["loc"]]) + "' "
            if loc == "'*' ":
                loc = ""
            return f"validation error: {loc}{details['msg']}"
        return f"validation error: {super().__str__()}"


class Unauthorized(Exception):
    pass


class PermissionDenied(Exception):
    pass
