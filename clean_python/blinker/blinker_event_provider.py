# (c) Nelen & Schuurmans
from typing import TypeVar

import blinker

from clean_python import DomainEvent
from clean_python import event_handler_registry
from clean_python import EventProvider

T = TypeVar("T", bound=DomainEvent)

__all__ = ["BlinkerEventProvider"]


class BlinkerEventProvider(EventProvider):
    def __init__(self):
        self._connected = False

    def connect(self) -> None:
        for path, handler in event_handler_registry:
            self._signal(path).connect(handler)
        self._connected = True

    def _signal(self, path: tuple[str, ...]) -> blinker.Signal:
        return blinker.signal(".".join(path))

    def send(self, event: DomainEvent) -> None:
        assert self._connected, "Event provider not connected"
        self._signal(event.__class__.event_path).send(event)

    async def send_async(self, event: DomainEvent) -> None:
        assert self._connected, "Event provider not connected"
        await self._signal(event.__class__.event_path).send_async(event)
