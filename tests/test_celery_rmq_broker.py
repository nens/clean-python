from unittest import mock

import pytest

from clean_python.celery import CeleryRmqBroker


@pytest.fixture
def celery_rmq_broker():
    return CeleryRmqBroker("amqp://rmq:1234//", "some_queue", "host", False)


@mock.patch("clean_python.celery.celery_rmq_broker.pika.BlockingConnection")
async def test_celery_rmq_broker(connection, celery_rmq_broker):
    await celery_rmq_broker.add({"task": "some.task", "args": ["foo", 15]})

    channel = connection().__enter__().channel()

    _, call_kwargs = channel.basic_publish.call_args

    assert call_kwargs["exchange"] == ""
    assert call_kwargs["routing_key"] == "some_queue"
    assert call_kwargs["body"] == '[["foo", 15], {}, null]'
    task_id = call_kwargs["properties"].correlation_id

    assert call_kwargs["properties"].headers["id"] == task_id
    assert call_kwargs["properties"].headers["root_id"] == task_id
    assert call_kwargs["properties"].headers["parent_id"] is None
    assert call_kwargs["properties"].headers["group"] is None
    assert call_kwargs["properties"].headers["lang"] == "py"
    assert call_kwargs["properties"].headers["task"] == "some.task"
    assert call_kwargs["properties"].headers["origin"] == "host"
    assert call_kwargs["properties"].headers["argsrepr"] == '["foo", 15]'
    assert call_kwargs["properties"].headers["kwargsrepr"] == "{}"
