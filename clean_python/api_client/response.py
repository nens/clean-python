from http import HTTPStatus
from typing import Optional

from clean_python import ValueObject

__all__ = ["Response"]


class Response(ValueObject):
    status: HTTPStatus
    data: bytes
    content_type: Optional[str]
