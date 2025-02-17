# (c) Nelen & Schuurmans
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeVar

import blinker

from clean_python import DomainEvent
from clean_python import EventProvider

T = TypeVar("T", bound=DomainEvent)


class BlinkerEventProvider(EventProvider):
    def _signal(self, event_type: type[DomainEvent]) -> blinker.Signal:
        return blinker.signal(event_type.event_path)

    def send(self, event: DomainEvent) -> None:
        self._signal(event.__class__).send(event)

    async def send_async(self, event: DomainEvent) -> None:
        await self._signal(event.__class__).send_async(event)

    def register_handler(
        self, event_type: type[T], receiver: Callable[[T], None | Awaitable[None]]
    ) -> Callable[[T], None | Awaitable[None]]:
        return self._signal(event_type).connect(receiver)
