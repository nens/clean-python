from typing import Iterator
from unittest.mock import AsyncMock
from unittest.mock import Mock

import inject
import pytest

from clean_python import DomainEvent
from clean_python import EventProvider
from clean_python import register_handler
from clean_python.base.domain.domain_event import clear_handlers
from clean_python.blinker import BlinkerEventProvider


class TestEvent(DomainEvent, path="some.path"):
    foo: str


@pytest.fixture
def event_provider() -> Iterator[EventProvider]:
    obj = BlinkerEventProvider()
    inject.configure(lambda binder: binder.bind(EventProvider, obj))
    yield obj
    inject.clear()


@pytest.fixture
def clear_registry():
    yield
    clear_handlers()


@pytest.mark.usefixtures("clear_registry")
def test_event_handler(event_provider: EventProvider):
    # at import time: define handlers and register them
    handler = Mock()
    TestEvent.register_handler(handler)

    # at bootstrap time: connect the event provider
    event_provider.connect()

    # at runtime: define and send the event
    event = TestEvent(foo="bar")
    event.send()

    handler.assert_called_once_with(event)


@pytest.mark.usefixtures("clear_registry")
def test_event_handler_different_path(event_provider: EventProvider):
    # at import time: define handlers and register them
    handler = Mock()
    register_handler(("other",), handler)

    # at bootstrap time: connect the event provider
    event_provider.connect()

    # at runtime: define and send the event
    event = TestEvent(foo="bar")
    event.send()

    assert not handler.called


@pytest.mark.usefixtures("clear_registry")
def test_event_two_handlers(event_provider: EventProvider):
    # at import time: define handlers and register them
    handler1 = Mock()
    handler2 = Mock()
    TestEvent.register_handler(handler1)
    TestEvent.register_handler(handler2)

    # at bootstrap time: connect the event provider
    event_provider.connect()

    # at runtime: define and send the event
    event = TestEvent(foo="bar")
    event.send()

    handler1.assert_called_once_with(event)
    handler2.assert_called_once_with(event)


@pytest.mark.usefixtures("clear_registry")
async def test_event_async_handler(event_provider: EventProvider):
    # at import time: define handlers and register them
    handler = AsyncMock()
    TestEvent.register_handler(handler)

    # at bootstrap time: connect the event provider
    event_provider.connect()

    # at runtime: define and send the event
    event = TestEvent(foo="bar")
    await event.send_async()

    handler.assert_awaited_once_with(event)


def test_send_not_connected(event_provider: EventProvider):
    event = TestEvent(foo="bar")
    with pytest.raises(AssertionError):
        event.send()


async def test_send_async_not_connected(event_provider: EventProvider):
    event = TestEvent(foo="bar")
    with pytest.raises(AssertionError):
        await event.send_async()
