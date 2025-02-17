# (c) Nelen & Schuurmans
from abc import ABC
from abc import abstractmethod
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeVar

import inject

from .value_object import ValueObject

__all__ = ["DomainEvent", "EventProvider"]


T = TypeVar("T", bound="DomainEvent")


class EventProvider(ABC):
    @abstractmethod
    def send(self, event: "DomainEvent") -> None:
        pass

    @abstractmethod
    async def send_async(self, event: "DomainEvent") -> None:
        pass

    @abstractmethod
    def register_handler(
        self, event: type[T], receiver: Callable[[T], None | Awaitable[None]]
    ) -> Callable[[T], None | Awaitable[None]]:
        pass


class DomainEvent(ValueObject):
    def send(self) -> None:
        inject.instance(EventProvider).send(self)

    async def send_async(self) -> None:
        await inject.instance(EventProvider).send_async(self)
