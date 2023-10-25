import os
from unittest import mock
from uuid import uuid4

import pytest
from dramatiq.errors import Retry
from dramatiq.message import Message

from clean_python import ctx
from clean_python import InMemoryGateway
from clean_python.dramatiq import DramatiqTaskLogger


@pytest.fixture
def in_memory_gateway():
    return InMemoryGateway(data=[])


@pytest.fixture
def task_logger(in_memory_gateway):
    return DramatiqTaskLogger(
        hostname="host",
        gateway_override=in_memory_gateway,
    )


@pytest.fixture
def correlation_id():
    uid = uuid4()
    ctx.correlation_id = uid
    yield uid
    ctx.correlation_id = None


@pytest.fixture
def patched_time():
    with mock.patch("time.time", side_effect=(0, 123.456)):
        yield


@pytest.fixture
def message():
    return Message(
        queue_name="default",
        actor_name="my_task",
        args=(1, 2),
        kwargs={"foo": "bar"},
        options={},
        message_id="abc123",
        message_timestamp=None,
    )


@pytest.fixture
def expected(correlation_id):
    return {
        "id": 1,
        "tag_suffix": "task_log",
        "task_id": "abc123",
        "name": "my_task",
        "state": "SUCCESS",
        "duration": 123.456,
        "retries": 0,
        "origin": f"host-{os.getpid()}",
        "argsrepr": b"[1,2]",
        "kwargsrepr": b'{"foo":"bar"}',
        "result": None,
        "time": 0.0,
        "correlation_id": str(correlation_id),
    }


async def test_log_success(
    patched_time, task_logger, in_memory_gateway, message, expected
):
    await task_logger.start()
    await task_logger.stop(message)

    assert in_memory_gateway.data[1] == expected


async def test_log_fail(
    patched_time, task_logger, in_memory_gateway, message, expected
):
    await task_logger.start()
    await task_logger.stop(message, exception=ValueError("test"))

    assert in_memory_gateway.data[1] == {
        **expected,
        "state": "FAILURE",
        "result": None,
    }


async def test_log_retry(
    patched_time, task_logger, in_memory_gateway, message, expected
):
    await task_logger.start()
    await task_logger.stop(message, exception=Retry("test"))

    assert in_memory_gateway.data[1] == {
        **expected,
        "state": "RETRY",
    }
