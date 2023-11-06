import time

import pytest
from celery.exceptions import Ignore
from celery.exceptions import Reject

from clean_python import InMemorySyncGateway
from clean_python.celery import BaseTask
from clean_python.celery import CeleryTaskLogger
from clean_python.celery import set_task_logger


@pytest.fixture(scope="session")
def celery_parameters():
    return {"task_cls": BaseTask}


@pytest.fixture(scope="session")
def celery_worker_parameters():
    return {"shutdown_timeout": 10}


@pytest.fixture
def celery_task(celery_app, celery_worker):
    @celery_app.task(bind=True, base=BaseTask, name="testing")
    def sleep_task(self: BaseTask, seconds: float, return_value=None, event="success"):
        event = event.lower()
        if event == "success":
            time.sleep(int(seconds))
        elif event == "crash":
            import ctypes

            ctypes.string_at(0)  # segfault
        elif event == "ignore":
            raise Ignore()
        elif event == "reject":
            raise Reject()
        elif event == "retry":
            raise self.retry(countdown=seconds, max_retries=1)
        else:
            raise ValueError(f"Unknown event '{event}'")

        return {"value": return_value}

    celery_worker.reload()
    return sleep_task


@pytest.fixture
def task_logger():
    logger = CeleryTaskLogger(InMemorySyncGateway([]))
    set_task_logger(logger)
    yield logger
    set_task_logger(None)


def test_log_success(celery_task: BaseTask, task_logger: CeleryTaskLogger):
    result = celery_task.delay(0.0, return_value=16)

    assert result.get(timeout=10) == {"value": 16}

    (log,) = task_logger.gateway.filter([])
    assert 0.0 < (time.time() - log["time"]) < 1.0
    assert log["tag_suffix"] == "task_log"
    assert log["task_id"] == result.id
    assert log["state"] == "SUCCESS"
    assert log["name"] == "testing"
    assert log["duration"] > 0.0
    assert log["argsrepr"] == "(0.0,)"
    assert log["kwargsrepr"] == "{'return_value': 16}"
    assert log["retries"] == 0
    assert log["result"] == {"value": 16}


def test_log_failure(celery_task: BaseTask, task_logger: CeleryTaskLogger):
    result = celery_task.delay(0.0, event="failure")

    with pytest.raises(ValueError):
        assert result.get(timeout=10)

    (log,) = task_logger.gateway.filter([])
    assert log["state"] == "FAILURE"
    assert log["result"]["traceback"].startswith("Traceback")
