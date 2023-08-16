# (c) Nelen & Schuurmans

import os
from contextvars import ContextVar
from typing import FrozenSet
from typing import Optional

from pydantic import AnyUrl
from pydantic import FileUrl

from .value_object import ValueObject

__all__ = ["ctx", "User", "Tenant", "Scope"]


class User(ValueObject):
    id: str
    name: str


Scope = FrozenSet[str]


class Tenant(ValueObject):
    id: int
    name: str


class Context:
    """Provide global access to some contextual properties.

    The implementation makes use of python's contextvars, which automatically integrates
    with asyncio tasks (so that each task runs in its own context). This makes sure that
    every request-response cycle is isolated.
    """

    def __init__(self):
        self._path_value: ContextVar[AnyUrl] = ContextVar(
            "path_value",
            default=FileUrl.build(scheme="file", host="/", path=os.getcwd()),
        )
        self._user_value: ContextVar[User] = ContextVar(
            "user_value", default=User(id="ANONYMOUS", name="anonymous")
        )
        self._tenant_value: ContextVar[Optional[Tenant]] = ContextVar(
            "tenant_value", default=None
        )

    def reset(self):
        self._path_value.reset()
        self._user_value.reset()
        self._tenant_value.reset()

    @property
    def path(self) -> AnyUrl:
        return self._path_value.get()

    @path.setter
    def path(self, value: AnyUrl) -> None:
        self._path_value.set(value)

    @property
    def user(self) -> User:
        return self._user_value.get()

    @user.setter
    def user(self, value: User) -> None:
        self._user_value.set(value)

    @property
    def tenant(self) -> Optional[Tenant]:
        return self._tenant_value.get()

    @tenant.setter
    def tenant(self, value: Optional[Tenant]) -> None:
        self._tenant_value.set(value)


ctx = Context()