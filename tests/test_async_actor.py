import threading
from asyncio import BaseEventLoop
from contextvars import ContextVar
from unittest import mock

import pytest

from clean_python.dramatiq import async_actor
from clean_python.dramatiq import AsyncActor
from clean_python.dramatiq import AsyncMiddleware
from clean_python.dramatiq.async_actor import EventLoopThread


@pytest.fixture
def started_thread():
    thread = EventLoopThread()
    thread.start()
    yield thread
    thread.join()


def test_event_loop_thread_start():
    try:
        thread = EventLoopThread()
        thread.start()
        assert isinstance(thread.loop, BaseEventLoop)
        assert thread.loop.is_running()
    finally:
        thread.join()


def test_event_loop_thread_run_coroutine(started_thread: EventLoopThread):
    result = {}

    async def get_thread_id():
        result["thread_id"] = threading.get_ident()

    started_thread.run_coroutine(get_thread_id())

    # the coroutine executed in the event loop thread
    assert result["thread_id"] == started_thread.ident


def test_event_loop_thread_run_coroutine_exception(started_thread: EventLoopThread):
    async def raise_error():
        raise TypeError("bla")

    coro = raise_error()

    with pytest.raises(TypeError, match="bla"):
        started_thread.run_coroutine(coro)


@mock.patch.object(EventLoopThread, "start")
@mock.patch.object(EventLoopThread, "run_coroutine")
def test_async_middleware_before_worker_boot(
    EventLoopThread_run_coroutine, EventLoopThread_start
):
    broker = mock.Mock()
    worker = mock.Mock()
    middleware = AsyncMiddleware()

    middleware.before_worker_boot(broker, worker)

    assert isinstance(middleware.event_loop_thread, EventLoopThread)

    EventLoopThread_start.assert_called_once()

    middleware.run_coroutine("foo")
    EventLoopThread_run_coroutine.assert_called_once_with("foo")

    # broker was patched with run_coroutine
    broker.run_coroutine("bar")
    EventLoopThread_run_coroutine.assert_called_with("bar")


def test_async_middleware_after_worker_shutdown():
    broker = mock.Mock()
    broker.run_coroutine = lambda x: x
    worker = mock.Mock()
    event_loop_thread = mock.Mock()

    middleware = AsyncMiddleware()
    middleware.event_loop_thread = event_loop_thread
    middleware.after_worker_shutdown(broker, worker)

    event_loop_thread.join.assert_called_once()
    assert middleware.event_loop_thread is None
    assert not hasattr(broker, "run_coroutine")


def test_async_actor():
    broker = mock.Mock()
    broker.actor_options = {"max_retries"}

    @async_actor(broker=broker)
    async def foo(*args, **kwargs):
        pass

    assert isinstance(foo, AsyncActor)

    foo(2, a="b")

    broker.run_coroutine.assert_called_once()

    # no recursion errors here:
    repr(foo)


foo_var: ContextVar[int] = ContextVar("foo", default=42)


def test_run_coroutine_keeps_context(started_thread: EventLoopThread):
    async def return_foo_var():
        return foo_var.get()

    foo_var.set(31)

    assert started_thread.run_coroutine(return_foo_var()) == 31
