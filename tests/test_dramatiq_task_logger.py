import os
from unittest import mock

import pytest
from dramatiq.errors import Retry
from dramatiq.message import Message

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
def expected():
    return {
        "id": 1,
        "tag_suffix": "task_log",
        "task_id": "abc123",
        "name": "my_task",
        "state": "SUCCESS",
        "duration": 0,
        "retries": 0,
        "origin": f"host-{os.getpid()}",
        "argsrepr": b"[1,2]",
        "kwargsrepr": b'{"foo":"bar"}',
        "result": None,
    }


@mock.patch("time.time", return_value=123)
async def test_log_success(time, task_logger, in_memory_gateway, message, expected):
    await task_logger.start()
    await task_logger.stop(message)

    assert in_memory_gateway.data[1] == expected


@mock.patch("time.time", new=mock.Mock(return_value=123))
async def test_log_fail(task_logger, in_memory_gateway, message, expected):
    await task_logger.start()
    await task_logger.stop(message, exception=ValueError("test"))

    assert in_memory_gateway.data[1] == {
        **expected,
        "state": "FAILURE",
        "result": None,
    }


@mock.patch("time.time", return_value=123)
async def test_log_retry(time, task_logger, in_memory_gateway, message, expected):
    await task_logger.start()
    await task_logger.stop(message, exception=Retry("test"))

    assert in_memory_gateway.data[1] == {
        **expected,
        "state": "RETRY",
    }
