# (c) Nelen & Schuurmans

import asyncio
import os

import pytest


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    if os.environ.get("DEBUG") or os.environ.get("DEBUG_WAIT_FOR_CLIENT"):
        from clean_python.testing.debugger import setup_debugger

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
async def postgres_url():
    return os.environ.get("POSTGRES_URL", "postgres:postgres@localhost:5432")


@pytest.fixture(scope="session")
async def s3_url():
    return os.environ.get("S3_URL", "http://localhost:9000")
