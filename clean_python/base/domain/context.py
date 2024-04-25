# (c) Nelen & Schuurmans

import os
from contextvars import ContextVar
from uuid import UUID

from pydantic import AnyUrl
from pydantic import FileUrl

from .types import Id
from .value_object import ValueObject

__all__ = ["ctx", "User", "Tenant", "Scope"]


class User(ValueObject):
    id: Id
    name: str


Scope = frozenset[str]


class Tenant(ValueObject):
    id: Id
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
        self._tenant_value: ContextVar[Tenant | None] = ContextVar(
            "tenant_value", default=None
        )
        self._correlation_id_value: ContextVar[UUID | None] = ContextVar(
            "correlation_id", default=None
        )

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
    def tenant(self) -> Tenant | None:
        return self._tenant_value.get()

    @tenant.setter
    def tenant(self, value: Tenant | None) -> None:
        self._tenant_value.set(value)

    @property
    def correlation_id(self) -> UUID | None:
        return self._correlation_id_value.get()

    @correlation_id.setter
    def correlation_id(self, value: UUID | None) -> None:
        self._correlation_id_value.set(value)


ctx = Context()
