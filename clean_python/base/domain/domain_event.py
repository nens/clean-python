# (c) Nelen & Schuurmans
from typing import Awaitable
from typing import Callable
from typing import Type
from typing import TypeVar

import blinker

__all__ = ["DomainEvent"]


T = TypeVar("T", bound="DomainEvent")


class DomainEvent:
    @classmethod
    def _signal(cls) -> blinker.Signal:
        return blinker.signal(cls.__name__)

    @classmethod
    def register_handler(
        cls: Type[T], receiver: Callable[[T], Awaitable[None]]
    ) -> Callable[[T], Awaitable[None]]:
        return cls._signal().connect(receiver)

    async def send_async(self) -> None:
        await self._signal().send_async(self)
