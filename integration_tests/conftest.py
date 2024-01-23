# (c) Nelen & Schuurmans

import asyncio
import multiprocessing
import os
import time
from urllib.error import URLError
from urllib.request import urlopen

import pytest
import uvicorn


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


@pytest.fixture(scope="session")
async def postgres_db_url(postgres_url) -> str:
    from sql_model import test_model
    from sqlalchemy import create_engine
    from sqlalchemy import text

    dbname = "cleanpython_test"
    root_engine = create_engine(
        f"postgresql+psycopg2://{postgres_url}", isolation_level="AUTOCOMMIT"
    )
    with root_engine.connect() as connection:
        connection.execute(text(f"DROP DATABASE IF EXISTS {dbname}"))
        connection.execute(text(f"CREATE DATABASE {dbname}"))
    root_engine.dispose()

    engine = create_engine(
        f"postgresql+psycopg2://{postgres_url}/{dbname}", isolation_level="AUTOCOMMIT"
    )
    with engine.connect() as connection:
        test_model.metadata.drop_all(engine)
        test_model.metadata.create_all(engine)
    engine.dispose()
    return f"{postgres_url}/{dbname}"


def wait_until_url_available(url: str, max_tries=10, interval=0.1):
    # wait for the server to be ready
    for _ in range(max_tries):
        try:
            urlopen(url)
        except URLError:
            time.sleep(interval)
            continue
        else:
            break


@pytest.fixture(scope="session")
async def fastapi_example_app():
    port = int(os.environ.get("API_PORT", "8005"))
    config = uvicorn.Config("fastapi_example:app", host="0.0.0.0", port=port)
    p = multiprocessing.Process(target=uvicorn.Server(config).run)
    p.start()
    try:
        wait_until_url_available(f"http://localhost:{port}/docs")
        yield f"http://localhost:{port}"
    finally:
        p.terminate()
