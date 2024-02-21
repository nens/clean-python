# (c) Nelen & Schuurmans

import logging

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

logger = logging.getLogger(__name__)

__all__ = [
    "ValidationErrorResponse",
    "DefaultErrorResponse",
    "not_found_handler",
    "conflict_handler",
    "validation_error_handler",
    "permission_denied_handler",
    "unauthorized_handler",
]


class ValidationErrorEntry(ValueObject):
    loc: list[str | int]
    msg: str
    type: str


class ValidationErrorResponse(ValueObject):
    message: str
    detail: list[ValidationErrorEntry]


class DefaultErrorResponse(ValueObject):
    message: str
    detail: str | None


async def not_found_handler(request: Request, exc: DoesNotExist) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={
            "message": f"Could not find {exc.name}{' with id=' + str(exc.id) if exc.id else ''}"
        },
    )


async def conflict_handler(request: Request, exc: Conflict) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={
            "message": "Conflict",
            "detail": jsonable_encoder(exc.args[0] if exc.args else None),
        },
    )


async def validation_error_handler(request: Request, exc: BadRequest) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=ValidationErrorResponse(
            message="Validation error", detail=exc.errors()  # type: ignore
        ).model_dump(mode="json"),
    )


async def unauthorized_handler(request: Request, exc: Unauthorized) -> JSONResponse:
    if exc.args:
        logger.info(f"unauthorized: {exc}")
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"message": "Unauthorized", "detail": None},
        headers={"WWW-Authenticate": "Bearer"},
    )


async def permission_denied_handler(
    request: Request, exc: PermissionDenied
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_403_FORBIDDEN,
        content={
            "message": "Permission denied",
            "detail": jsonable_encoder(exc.args[0] if exc.args else None),
        },
    )
