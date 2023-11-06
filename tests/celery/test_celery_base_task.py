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


def test_apply_async(mocked_apply_async):
    BaseTask().apply_async(args="foo", kwargs="bar")

    assert mocked_apply_async.call_count == 1
    args, kwargs = mocked_apply_async.call_args
    assert args == ("foo", "bar")
    assert kwargs["headers"][HEADER_FIELD]["tenant"] is None
    UUID(kwargs["headers"][HEADER_FIELD]["correlation_id"])  # generated


def test_apply_async_with_context(mocked_apply_async, temp_context):
    BaseTask().apply_async(args="foo", kwargs="bar")

    assert mocked_apply_async.call_count == 1
    _, kwargs = mocked_apply_async.call_args
    assert kwargs["headers"][HEADER_FIELD]["tenant"] == temp_context.tenant.model_dump(
        mode="json"
    )
    kwargs["headers"][HEADER_FIELD]["correlation_id"] == str(
        temp_context.correlation_id
    )


def test_apply_async_headers_extended(mocked_apply_async):
    headers = {"baz": 2}
    BaseTask().apply_async(args="foo", kwargs="bar", headers=headers)

    assert mocked_apply_async.call_count == 1
    _, kwargs = mocked_apply_async.call_args
    assert kwargs["headers"]["baz"] == 2
    assert kwargs["headers"][HEADER_FIELD]["tenant"] is None
    UUID(kwargs["headers"][HEADER_FIELD]["correlation_id"])  # generated

    assert headers == {"baz": 2}  # not changed inplace


def test_apply_async_headers_already_present(mocked_apply_async):
    BaseTask().apply_async(args="foo", kwargs="bar", headers={HEADER_FIELD: "foo"})

    assert mocked_apply_async.call_count == 1
    _, kwargs = mocked_apply_async.call_args
    assert kwargs["headers"] == {HEADER_FIELD: "foo"}
