from unittest import mock

from clean_python.fastapi.service import to_lifespan


async def test_to_lifespan_empty_lists():
    lifespan = to_lifespan([], [])

    async with lifespan(None):
        pass


async def test_to_lifespan_startup_func():
    on_startup = mock.Mock()
    lifespan = to_lifespan([on_startup], [])

    async with lifespan(None):
        on_startup.assert_called_once()


async def test_to_lifespan_startup_coroutine():
    on_startup = mock.AsyncMock()
    lifespan = to_lifespan([on_startup], [])

    async with lifespan(None):
        on_startup.assert_awaited_once()


async def test_to_lifespan_startup_mixed():
    on_startup_sync = mock.Mock()
    on_startup_async = mock.AsyncMock()
    lifespan = to_lifespan([on_startup_sync, on_startup_async], [])

    async with lifespan(None):
        on_startup_sync.assert_called_once()
        on_startup_async.assert_awaited_once()


async def test_to_lifespan_shutdown_func():
    on_shutdown = mock.Mock()
    lifespan = to_lifespan([], [on_shutdown])

    async with lifespan(None):
        assert not on_shutdown.called

    on_shutdown.assert_called_once()


async def test_to_lifespan_shutdown_coroutine():
    on_shutdown = mock.AsyncMock()
    lifespan = to_lifespan([], [on_shutdown])

    async with lifespan(None):
        assert not on_shutdown.called

    on_shutdown.assert_awaited_once()


async def test_to_lifespan_shutdown_mixed():
    on_shutdown_sync = mock.Mock()
    on_shutdown_async = mock.AsyncMock()
    lifespan = to_lifespan([], [on_shutdown_sync, on_shutdown_async])

    async with lifespan(None):
        assert not on_shutdown_sync.called
        assert not on_shutdown_async.called

    on_shutdown_sync.assert_called_once()
    on_shutdown_async.assert_awaited_once()
