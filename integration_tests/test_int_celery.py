import time
from uuid import UUID

import pytest
from celery.exceptions import Ignore
from celery.exceptions import Reject

from clean_python import ctx
from clean_python import InMemorySyncGateway
from clean_python import Tenant
from clean_python.celery import BaseTask
from clean_python.celery import CeleryTaskLogger
from clean_python.celery import set_task_logger


@pytest.fixture(scope="session")
def celery_parameters():
    return {"task_cls": BaseTask, "strict_typing": False}


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
        elif event == "context":
            return {
                "tenant_id": ctx.tenant.id,
                "correlation_id": str(ctx.correlation_id),
            }
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
    assert log["args"] == [0.0]
    assert log["kwargs"] == {"return_value": 16}
    assert log["retries"] == 0
    assert log["result"] == {"value": 16}
    assert UUID(log["correlation_id"])  # generated
    assert log["tenant_id"] is None


def test_log_failure(celery_task: BaseTask, task_logger: CeleryTaskLogger):
    result = celery_task.delay(0.0, event="failure")

    with pytest.raises(ValueError):
        assert result.get(timeout=10)

    (log,) = task_logger.gateway.filter([])
    assert log["state"] == "FAILURE"
    assert log["result"]["traceback"].startswith("Traceback")


@pytest.fixture
def custom_context():
    ctx.correlation_id = UUID("b3089ea7-2585-43e5-a63c-ae30a6e9b5e4")
    ctx.tenant = Tenant(id=2, name="custom")
    yield ctx
    ctx.correlation_id = None
    ctx.tenant = None


def test_context(celery_task: BaseTask, custom_context, task_logger):
    result = celery_task.apply_async((0.0,), {"event": "context"}, countdown=1.0)

    assert result.get(timeout=10) == {
        "tenant_id": 2,
        "correlation_id": "b3089ea7-2585-43e5-a63c-ae30a6e9b5e4",
    }

    (log,) = task_logger.gateway.filter([])
    assert log["correlation_id"] == "b3089ea7-2585-43e5-a63c-ae30a6e9b5e4"
    assert log["tenant_id"] == 2
