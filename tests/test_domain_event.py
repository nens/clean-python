from typing import Iterator
from unittest.mock import Mock
from unittest.mock import patch

import inject
import pytest

from clean_python import DomainEvent
from clean_python import EventProvider
from clean_python import register_handler
from clean_python.base.domain.domain_event import clear_handlers
from clean_python.base.domain.domain_event import event_handler_registry


class TestEvent(DomainEvent, path="test"):
    foo: str


MODULE = "clean_python.base.domain.domain_event"


@pytest.fixture
def event() -> TestEvent:
    return TestEvent(foo="bar")


@pytest.fixture
def event_provider() -> Iterator[EventProvider]:
    obj = Mock(EventProvider)
    inject.configure(lambda binder: binder.bind(EventProvider, obj))
    yield obj
    inject.clear()


@pytest.fixture
def clear_registry():
    yield
    clear_handlers()


def test_event_subclass():
    class TestEvent(DomainEvent):
        pass

    assert TestEvent.event_path == ("TestEvent",)


def test_event_subclass_with_path():
    class TestEvent(DomainEvent, path="some.path"):
        pass

    assert TestEvent.event_path == ("some", "path")


def test_event_double_subclass():
    class TestEvent2(TestEvent):
        pass

    assert TestEvent2.event_path == ("test", "TestEvent2")
    assert TestEvent.event_path == ("test",)  # unchanged


def test_event_double_subclass_with_path():
    class TestEvent2(TestEvent, path="some.path"):
        pass

    assert TestEvent2.event_path == ("test", "some", "path")
    assert TestEvent.event_path == ("test",)  # unchanged


def test_event_init():
    event = TestEvent(foo="bar")
    assert event.foo == "bar"


def test_event_send(event_provider: Mock, event: TestEvent):
    event.send()
    event_provider.send.assert_called_once_with(event)


async def test_event_send_async(event_provider: Mock, event: TestEvent):
    await event.send_async()
    event_provider.send_async.assert_awaited_once_with(event)


@patch(MODULE + ".register_handler")
def test_register_handler_method(register_handler_mock: Mock):
    def handler(event: TestEvent):
        pass

    assert TestEvent.register_handler(handler) is register_handler_mock.return_value

    register_handler_mock.assert_called_once_with(TestEvent.event_path, handler)


@pytest.mark.usefixtures("clear_registry")
def test_register_handler():
    def handler(event: TestEvent):
        pass

    register_handler(("foo", "bar"), handler)

    assert len(event_handler_registry) == 1
    assert event_handler_registry == {(("foo", "bar"), handler)}


@pytest.mark.usefixtures("clear_registry")
def test_register_same_handler_twice():
    def handler(event: TestEvent):
        pass

    register_handler(("foo", "bar"), handler)
    register_handler(("foo", "bar"), handler)

    assert len(event_handler_registry) == 1


@pytest.mark.usefixtures("clear_registry")
def test_register_same_handler_different_paths():
    def handler(event: TestEvent):
        pass

    register_handler(("foo", "bar"), handler)
    register_handler(("foo", "baz"), handler)

    assert len(event_handler_registry) == 2


@pytest.mark.usefixtures("clear_registry")
def test_register_different_handler_same_paths():
    register_handler(("foo", "bar"), lambda x: 1)
    register_handler(("foo", "bar"), lambda x: 2)

    assert len(event_handler_registry) == 2
