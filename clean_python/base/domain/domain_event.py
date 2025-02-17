# (c) Nelen & Schuurmans
from abc import ABC
from abc import abstractmethod
from collections.abc import Awaitable
from collections.abc import Callable
from typing import ClassVar
from typing import TypeVar

import inject

from .provider import SyncProvider
from .value_object import ValueObject

__all__ = [
    "DomainEvent",
    "EventProvider",
    "register_handler",
    "EventHandler",
    "event_handler_registry",
]


T = TypeVar("T", bound="DomainEvent")
EventHandler = Callable[["DomainEvent"], None | Awaitable[None]]
event_handler_registry: set[tuple[tuple[str, ...], EventHandler]] = set()


def register_handler(path: tuple[str, ...], receiver: EventHandler) -> EventHandler:
    event_handler_registry.add((path, receiver))
    return receiver


def clear_handlers() -> None:
    event_handler_registry.clear()


class EventProvider(SyncProvider, ABC):
    @abstractmethod
    def send(self, event: "DomainEvent") -> None:
        pass

    @abstractmethod
    async def send_async(self, event: "DomainEvent") -> None:
        pass


class DomainEvent(ValueObject):
    event_path: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls: type["DomainEvent"], path: str | None = None) -> None:
        if path is None:
            cls.event_path += (cls.__name__,)
        else:
            cls.event_path += tuple(path.split("."))
        super().__init_subclass__()

    def send(self) -> None:
        inject.instance(EventProvider).send(self)

    async def send_async(self) -> None:
        await inject.instance(EventProvider).send_async(self)

    @classmethod
    def register_handler(
        cls: type["DomainEvent"], receiver: EventHandler
    ) -> EventHandler:
        return register_handler(cls.event_path, receiver)
