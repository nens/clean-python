# (c) Nelen & Schuurmans

from typing import List
from typing import Union

from fastapi.encoders import jsonable_encoder
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from starlette import status

from clean_python import BadRequest
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python import ValueObject

__all__ = [
    "ValidationErrorResponse",
    "DefaultErrorResponse",
    "not_found_handler",
    "conflict_handler",
    "validation_error_handler",
    "not_implemented_handler",
    "permission_denied_handler",
    "unauthorized_handler",
]


class ValidationErrorEntry(ValueObject):
    loc: List[Union[str, int]]
    msg: str
    type: str


class ValidationErrorResponse(ValueObject):
    detail: List[ValidationErrorEntry]


class DefaultErrorResponse(ValueObject):
    message: str


async def not_found_handler(request: Request, exc: DoesNotExist):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"message": f"Could not find {exc.name} with id={exc.id}"},
    )


async def conflict_handler(request: Request, exc: Conflict):
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"message": str(exc)},
    )


async def validation_error_handler(request: Request, exc: BadRequest):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=jsonable_encoder({"detail": exc.errors()}),
    )


async def not_implemented_handler(request: Request, exc: NotImplementedError):
    return JSONResponse(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        content={"message": str(exc)},
    )


async def unauthorized_handler(request: Request, exc: Unauthorized):
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"message": "Unauthorized"},
    )


async def permission_denied_handler(request: Request, exc: PermissionDenied):
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={"message": "Permission denied"},
    )
