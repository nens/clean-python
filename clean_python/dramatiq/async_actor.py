# (c) Nelen & Schuurmans

import asyncio
import logging
import threading
import time
from concurrent.futures import TimeoutError
from typing import Any
from typing import Awaitable
from typing import Dict
from typing import Optional
from typing import TypeVar

import dramatiq
from asgiref.sync import sync_to_async
from dramatiq.brokers.stub import StubBroker
from dramatiq.middleware import Interrupt
from dramatiq.middleware import Middleware

__all__ = ["AsyncActor", "AsyncMiddleware", "async_actor"]


logger = logging.getLogger(__name__)

# Default broker (for testing)
broker = StubBroker()
broker.run_coroutine = lambda coro: asyncio.run(coro)
dramatiq.set_broker(broker)

R = TypeVar("R")


class EventLoopThread(threading.Thread):
    """A thread that starts / stops an asyncio event loop.

    The method 'run_coroutine' should be used to run coroutines from a
    synchronous context.
    """

    EVENT_LOOP_START_TIMEOUT = 0.1  # seconds to wait for the event loop to start

    loop: Optional[asyncio.AbstractEventLoop] = None

    def __init__(self):
        super().__init__(target=self._start_event_loop)

    def _start_event_loop(self):
        """This method should run in the thread"""
        logger.info("Starting the event loop...")

        self.loop = asyncio.new_event_loop()
        try:
            self.loop.run_forever()
        finally:
            self.loop.close()

    def _stop_event_loop(self):
        """This method should run outside of the thread"""
        if self.loop is not None:
            logger.info("Stopping the event loop...")
            self.loop.call_soon_threadsafe(self.loop.stop)

    def run_coroutine(self, coro: Awaitable[R]) -> R:
        """To be called from outside the thread

        Blocks until the coroutine is finished.
        """
        if self.loop is None or not self.loop.is_running():
            raise RuntimeError("The event loop is not running")

        done = threading.Event()

        async def wrapped_coro() -> R:
            try:
                return await coro
            finally:
                done.set()

        future = asyncio.run_coroutine_threadsafe(wrapped_coro(), self.loop)
        try:
            while True:
                try:
                    # Use a timeout to be able to catch asynchronously raised dramatiq
                    # exceptions (Shutdown and TimeLimitExceeded).
                    return future.result(timeout=1)
                except TimeoutError:
                    continue
        except Interrupt:
            self.loop.call_soon_threadsafe(future.cancel)
            # The future will raise a CancelledError *before* the coro actually
            # finished cleanup. Wait for the event instead.
            done.wait()
            raise

    def start(self, *args, **kwargs):
        super().start(*args, **kwargs)
        time.sleep(self.EVENT_LOOP_START_TIMEOUT)
        if self.loop is None or not self.loop.is_running():
            logger.exception("The event loop failed to start")
        logger.info("Event loop is running.")

    def join(self, *args, **kwargs):
        self._stop_event_loop()
        return super().join(*args, **kwargs)


class AsyncMiddleware(Middleware):
    """This middleware enables coroutines to be ran as dramatiq a actors.

    At its core, this middleware spins up a dedicated thread ('event_loop_thread'),
    which may be used to schedule the coroutines on from the worker threads.
    """

    event_loop_thread: Optional[EventLoopThread] = None

    def run_coroutine(self, coro: Awaitable[R]) -> R:
        assert self.event_loop_thread is not None
        return self.event_loop_thread.run_coroutine(coro)

    def before_worker_boot(self, broker, worker):
        self.event_loop_thread = EventLoopThread()
        self.event_loop_thread.start()

        broker.run_coroutine = self.run_coroutine

    def after_worker_shutdown(self, broker, worker):
        assert self.event_loop_thread is not None
        self.event_loop_thread.join()
        self.event_loop_thread = None

        delattr(broker, "run_coroutine")


class AsyncActor(dramatiq.Actor):
    """To configure coroutines as a dramatiq actor.

    Requires AsyncMiddleware to be active.

    Example usage:

    >>> @dramatiq.actor(..., actor_class=AsyncActor)
    ... async def my_task(x):
    ...     print(x)

    Notes:

    The async functions are scheduled on an event loop that is shared between
    worker threads. See AsyncMiddleware.

    This is compatible with ShutdownNotifications ("notify_shutdown") and
    TimeLimit ("time_limit"). Both result in an asyncio.CancelledError raised inside
    the async function. There is currently no way to tell the two apart.
    """

    def __init__(self, fn, *args, **kwargs):
        super().__init__(
            lambda *args, **kwargs: self.broker.run_coroutine(fn(*args, **kwargs)),
            *args,
            **kwargs,
        )

    @sync_to_async
    def send_async(self, *args, **kwargs) -> dramatiq.Message[R]:
        """See dramatiq.actor.Actor.send.

        Sending a message to a broker is potentially blocking, so @sync_to_async is used.
        """
        return super().send(*args, **kwargs)

    @sync_to_async
    def send_async_with_options(
        self,
        *,
        args: tuple = (),  # type: ignore
        kwargs: Optional[Dict[str, Any]] = None,
        delay: Optional[int] = None,
        **options,
    ) -> dramatiq.Message[R]:
        """See dramatiq.actor.Actor.send_with_options.

        Sending a message to a broker is potentially blocking, so @sync_to_async is used.
        """
        return super().send_with_options(
            args=args, kwargs=kwargs, delay=delay, **options
        )


def async_actor(awaitable=None, **kwargs):
    kwargs.setdefault("max_retries", 0)
    if awaitable:
        return dramatiq.actor(awaitable, actor_class=AsyncActor, **kwargs)
    else:

        def wrapper(awaitable):
            return dramatiq.actor(awaitable, actor_class=AsyncActor, **kwargs)

        return wrapper
