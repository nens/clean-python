# (c) Nelen & Schuurmans
from collections.abc import Awaitable
from collections.abc import Callable
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
        cls: type[T], receiver: Callable[[T], None | Awaitable[None]]
    ) -> Callable[[T], None | Awaitable[None]]:
        return cls._signal().connect(receiver)

    def send(self) -> None:
        self._signal().send(self)

    async def send_async(self) -> None:
        await self._signal().send_async(self)
