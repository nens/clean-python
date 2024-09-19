import json
import time
from uuid import UUID

import pytest
from celery.exceptions import MaxRetriesExceededError

from clean_python import ctx
from clean_python import InMemorySyncGateway
from clean_python import SyncGateway
from clean_python import Tenant
from clean_python.celery import CeleryTaskLogger
from clean_python.celery import set_task_logger

from .celery_example import sleep_task


@pytest.fixture
def task_logger():
    logger = CeleryTaskLogger(InMemorySyncGateway([]))
    set_task_logger(logger)
    yield logger
    set_task_logger(None)


@pytest.mark.usefixtures("celery_worker")
def test_run_task():
    result = sleep_task.delay(0.0, return_value=16)

    assert result.get(timeout=10) == {"value": 16}


@pytest.fixture
def custom_context():
    ctx.correlation_id = UUID("b3089ea7-2585-43e5-a63c-ae30a6e9b5e4")
    ctx.tenant = Tenant(id=2, name="custom")
    yield ctx
    ctx.correlation_id = None
    ctx.tenant = None


@pytest.mark.usefixtures("celery_worker")
def test_context(custom_context):
    result = sleep_task.delay(0.0, event="context")

    assert result.get(timeout=10) == {
        "tenant_id": custom_context.tenant.id,
        "correlation_id": str(custom_context.correlation_id),
    }


def test_log_success(celery_task_logs: SyncGateway):
    result = sleep_task.delay(0.0, return_value=16)

    assert result.get(timeout=10) == {"value": 16}

    (log,) = celery_task_logs.filter([])
    assert 0.0 < (time.time() - log["time"]) < 1.0
    assert log["tag_suffix"] == "task_log"
    assert log["task_id"] == result.id
    assert log["state"] == "SUCCESS"
    assert log["name"] == "testing"
    assert log["duration"] > 0.0
    assert json.loads(log["argsrepr"]) == [0.0]
    assert json.loads(log["kwargsrepr"]) == {"return_value": 16}
    assert log["retries"] == 0
    assert log["result"] == {"value": 16}
    assert UUID(log["correlation_id"])  # generated
    assert log["tenant_id"] is None


def test_log_failure(celery_task_logs: SyncGateway):
    result = sleep_task.delay(0.0, event="failure")

    with pytest.raises(ValueError):
        assert result.get(timeout=10)

    (log,) = celery_task_logs.filter([])
    assert log["state"] == "FAILURE"
    assert log["result"]["traceback"].startswith("Traceback")


def test_log_context(celery_task_logs: SyncGateway, custom_context):
    result = sleep_task.delay(0.0, return_value=16)

    assert result.get(timeout=10) == {"value": 16}

    (log,) = celery_task_logs.filter([])
    assert log["correlation_id"] == str(custom_context.correlation_id)
    assert log["tenant_id"] == custom_context.tenant.id


def test_log_retry_propagates_context(celery_task_logs: SyncGateway, custom_context):
    result = sleep_task.delay(0.0, event="retry")

    with pytest.raises(MaxRetriesExceededError):
        result.get(timeout=10)

    (log,) = celery_task_logs.filter([])
    assert log["state"] == "FAILURE"
    assert log["retries"] == 1
    assert log["correlation_id"] == str(custom_context.correlation_id)
    assert log["tenant_id"] == custom_context.tenant.id
