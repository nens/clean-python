from http import HTTPStatus

from clean_python import ValueObject

__all__ = ["Response"]


class Response(ValueObject):
    status: HTTPStatus
    data: bytes
    content_type: str | None
