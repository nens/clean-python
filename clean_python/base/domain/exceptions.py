# (c) Nelen & Schuurmans

from typing import Any
from typing import Optional
from typing import Union

from pydantic import create_model
from pydantic import ValidationError

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
    def __init__(self, name: str, id: Optional[int] = None):
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


class AlreadyExists(Conflict):
    def __init__(self, id: Optional[int] = None):
        super().__init__(f"record with id={id} already exists")


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

    def __str__(self) -> str:
        error = self._internal_error
        if isinstance(error, ValidationError):
            error = error.errors()[0]
            loc = "'" + ",".join([str(x) for x in error["loc"]]) + "' "
            if loc == "'*' ":
                loc = ""
            return f"validation error: {loc}{error['msg']}"
        return super().__str__()


class Unauthorized(Exception):
    pass


class PermissionDenied(Exception):
    pass
