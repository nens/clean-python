# (c) Nelen & Schuurmans

import asyncio
import io
import multiprocessing
import os
import signal
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import boto3
import pytest
import uvicorn
from botocore.exceptions import ClientError

from .celery_example import MultilineJsonFileGateway


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
    from sqlalchemy import create_engine
    from sqlalchemy import text

    from .sql_model import test_model

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
    config = uvicorn.Config(
        "integration_tests.fastapi_example:app", host="0.0.0.0", port=port
    )
    p = multiprocessing.Process(target=uvicorn.Server(config).run)
    p.start()
    try:
        wait_until_url_available(f"http://localhost:{port}/docs")
        yield f"http://localhost:{port}"
    finally:
        p.terminate()


@pytest.fixture(scope="session")
def celery_worker(tmp_path_factory):
    log_file = str(tmp_path_factory.mktemp("pytest-celery") / "celery.log")
    p = subprocess.Popen(
        [
            "celery",
            "-A",
            "integration_tests.celery_example",
            "worker",
            "-c",
            "1",
            # "-P",  enable when using the debugger
            # "solo"
        ],
        start_new_session=True,
        stdout=subprocess.PIPE,
        # optionally add "CLEAN_PYTHON_TEST_DEBUG": "5679" to enable debugging
        env={"CLEAN_PYTHON_TEST_LOGGING": log_file, **os.environ},
    )
    try:
        yield MultilineJsonFileGateway(Path(log_file))
    finally:
        p.send_signal(signal.SIGQUIT)


@pytest.fixture
def celery_task_logs(celery_worker):
    celery_worker.clear()
    return celery_worker


@pytest.fixture(scope="session")
def s3_settings(s3_url):
    minio_settings = {
        "url": s3_url,
        "access_key": "cleanpython",
        "secret_key": "cleanpython",
        "bucket": "cleanpython-test",
        "region": None,
    }
    if not minio_settings["bucket"].endswith("-test"):  # type: ignore
        pytest.exit("Not running against a test minio bucket?! 😱")
    return minio_settings.copy()


@pytest.fixture(scope="session")
def s3_bucket_session(s3_settings):
    s3 = boto3.resource(
        "s3",
        endpoint_url=s3_settings["url"],
        aws_access_key_id=s3_settings["access_key"],
        aws_secret_access_key=s3_settings["secret_key"],
    )
    bucket = s3.Bucket(s3_settings["bucket"])

    # ensure existence
    try:
        bucket.create()
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            pass
    return bucket


@pytest.fixture
def s3_bucket(s3_bucket_session):
    yield s3_bucket_session
    s3_bucket_session.objects.all().delete()


@pytest.fixture
def local_file(tmp_path):
    path = tmp_path / "test-upload.txt"
    path.write_bytes(b"foo")
    return path


@pytest.fixture
def object_in_s3(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "object-in-s3")
    return "object-in-s3"


@pytest.fixture
def object_in_s3_tenant(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "tenant-22/object-in-s3")
    return "object-in-s3"


@pytest.fixture
def object_in_s3_other_tenant(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "tenant-222/object-in-s3")
    return "object-in-s3"
