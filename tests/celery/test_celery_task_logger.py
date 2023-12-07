from unittest import mock
from uuid import uuid4

import pytest
from celery import Task

from clean_python import InMemorySyncGateway
from clean_python.celery import CeleryTaskLogger


@pytest.fixture
def celery_task_logger() -> CeleryTaskLogger:
    return CeleryTaskLogger(InMemorySyncGateway([]))


def test_log_minimal(celery_task_logger: CeleryTaskLogger):
    celery_task_logger.stop(Task(), "STAAT")
    (entry,) = celery_task_logger.gateway.filter([])
    assert entry == {
        "id": 1,
        "tag_suffix": "task_log",
        "task_id": None,
        "name": None,
        "state": "STAAT",
        "duration": None,
        "origin": None,
        "argsrepr": None,
        "kwargsrepr": None,
        "result": None,
        "time": None,
        "tenant_id": None,
        "correlation_id": None,
        "retries": None,
    }


def test_log_with_duration(celery_task_logger: CeleryTaskLogger):
    with mock.patch("time.time", return_value=1.0):
        celery_task_logger.start()

    with mock.patch("time.time", return_value=100.0):
        celery_task_logger.stop(Task(), "STAAT")

    (entry,) = celery_task_logger.gateway.filter([])
    assert entry["time"] == 1.0
    assert entry["duration"] == 99.0


@pytest.fixture
def celery_task():
    # it seems impossible to instantiate a true celery Task object...
    request = mock.Mock()
    request.id = "abc123"
    request.origin = "hostname"
    request.retries = 25
    request.args = [1, 2]
    request.kwargs = {
        "clean_python_context": {
            "tenant": None,
            "correlation_id": "b3089ea7-2585-43e5-a63c-ae30a6e9b5e4",
        }
    }
    task = mock.Mock()
    task.name = "task_name"
    task.request = request
    return task


def test_log_with_request(celery_task_logger: CeleryTaskLogger, celery_task):
    celery_task_logger.stop(celery_task, "STAAT")

    (entry,) = celery_task_logger.gateway.filter([])
    assert entry["name"] == "task_name"
    assert entry["task_id"] == "abc123"
    assert entry["retries"] == 25
    assert entry["argsrepr"] == "[1, 2]"
    assert entry["kwargsrepr"] == "{}"
    assert entry["origin"] == "hostname"
    assert entry["correlation_id"] == "b3089ea7-2585-43e5-a63c-ae30a6e9b5e4"


@pytest.mark.parametrize(
    "result,expected",
    [
        ({"a": "b"}, {"a": "b"}),
        ("str", {"result": "str"}),  # str to dict
        ([1], {"result": [1]}),  # list to dict
        ({"a": uuid4()}, None),  # not-json-serializable
    ],
)
def test_log_with_result(
    celery_task_logger: CeleryTaskLogger, celery_task, result, expected
):
    celery_task_logger.stop(celery_task, "STAAT", result=result)

    (entry,) = celery_task_logger.gateway.filter([])
    assert entry["result"] == expected
