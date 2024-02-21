# (c) Nelen & Schuurmans

from typing import Any

from pydantic import create_model
from pydantic import ValidationError
from pydantic_core import ErrorDetails

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
    def __init__(self, name: str, id: Id | None = None):
        super().__init__()
        self.name = name
        self.id = id

    def __str__(self):
        if self.id:
            return f"does not exist: {self.name} with id={self.id}"
        else:
            return f"does not exist: {self.name}"


class Conflict(Exception):
    def __init__(self, msg: str | None = None):
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
    def __init__(self, err_or_msg: ValidationError | str):
        self._internal_error = err_or_msg
        super().__init__(err_or_msg)

    def errors(self) -> list[ErrorDetails]:
        if isinstance(self._internal_error, ValidationError):
            return self._internal_error.errors()
        return [
            ErrorDetails(
                type="value_error",
                msg=self._internal_error,
                loc=[],  # type: ignore
                input=None,
            )
        ]

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
