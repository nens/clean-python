# (c) Nelen & Schuurmans

from abc import ABC
from typing import Awaitable
from typing import Callable
from typing import TypeVar

import blinker

__all__ = ["DomainEvent"]


TDomainEvent = TypeVar("TDomainEvent", bound="DomainEvent")
TEventHandler = Callable[[TDomainEvent], Awaitable[None]]


class DomainEvent(ABC):
    @classmethod
    def _signal(cls) -> blinker.Signal:
        return blinker.signal(cls.__name__)

    @classmethod
    def register_handler(cls, receiver: TEventHandler) -> TEventHandler:
        return cls._signal().connect(receiver)

    async def send_async(self) -> None:
        await self._signal().send_async(self)
