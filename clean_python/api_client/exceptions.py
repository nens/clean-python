from http import HTTPStatus
from typing import Any

__all__ = ["ApiException"]


class ApiException(ValueError):
    def __init__(self, obj: Any, status: HTTPStatus):
        self.status = status
        super().__init__(obj)
