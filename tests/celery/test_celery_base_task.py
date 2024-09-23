from unittest import mock
from uuid import UUID
from uuid import uuid4

import pytest
from celery import Task

from clean_python import ctx
from clean_python import Tenant
from clean_python.celery import BaseTask
from clean_python.celery.base_task import HEADER_FIELD


@pytest.fixture
def mocked_apply_async():
    with mock.patch.object(Task, "apply_async") as m:
        yield m


@pytest.fixture
def temp_context():
    ctx.tenant = Tenant(id=2, name="test")
    ctx.correlation_id = uuid4()
    yield ctx
    ctx.tenant = None
    ctx.correlation_id = None


@mock.patch(
    "clean_python.celery.base_task.uuid4",
    return_value=UUID("479156af-a302-48fc-89ed-8c426abadc4c"),
)
def test_apply_async(uuid4, mocked_apply_async):
    BaseTask().apply_async(args=("foo",), kwargs={"a": "bar"})

    assert mocked_apply_async.call_count == 1
    (args, kwargs), options = mocked_apply_async.call_args
    assert args == ("foo",)
    assert kwargs["a"] == "bar"
    assert options["headers"][HEADER_FIELD] == {
        "tenant_id": None,
        "correlation_id": "479156af-a302-48fc-89ed-8c426abadc4c",
    }


def test_apply_async_with_context(mocked_apply_async, temp_context):
    BaseTask().apply_async(args=("foo",), kwargs={"a": "bar"})

    assert mocked_apply_async.call_count == 1
    (_, kwargs), options = mocked_apply_async.call_args
    assert kwargs["a"] == "bar"
    assert options["headers"][HEADER_FIELD]["tenant_id"] == temp_context.tenant.id
    assert options["headers"][HEADER_FIELD]["correlation_id"] == str(
        temp_context.correlation_id
    )
