# (c) Nelen & Schuurmans

import asyncio

import pytest

from clean_python.sql import SQLDatabase
from clean_python.testing import setup_debugger


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    setup_debugger()


@pytest.fixture(scope="session")
def event_loop(request):
    """Create an instance of the default event loop per test session.

    Async fixtures need the event loop, and so must have the same or narrower scope than
    the event_loop fixture. Since we have async session-scoped fixtures, the default
    event_loop fixture, which has function scope, cannot be used. See:
    https://github.com/pytest-dev/pytest-asyncio#async-fixtures
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def postgres_url():
    return "postgresql+asyncpg://postgres:postgres@localhost:5432/cleanpython_test"


@pytest.fixture(scope="session")
async def provider(postgres_url):
    provider = SQLDatabase(postgres_url)
    # await provider.execute(text("DELETE FROM test_model WHERE TRUE RETURNING id"))
    yield provider
    # await provider.execute(text("DELETE FROM test_model WHERE TRUE RETURNING id"))
