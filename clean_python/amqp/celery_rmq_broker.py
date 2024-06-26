# (c) Nelen & Schuurmans

import json
import ssl
import uuid

import pika
from asgiref.sync import sync_to_async
from pydantic import AnyUrl

from clean_python import Gateway
from clean_python import Json
from clean_python import ValueObject

__all__ = ["CeleryRmqBroker"]


class CeleryHeaders(ValueObject):
    lang: str = "py"
    task: str
    id: uuid.UUID
    root_id: uuid.UUID
    parent_id: uuid.UUID | None = None
    group: uuid.UUID | None = None
    argsrepr: str | None = None
    kwargsrepr: str | None = None
    origin: str | None = None

    def json_dict(self):
        return json.loads(self.model_dump_json())


class CeleryRmqBroker(Gateway):
    def __init__(
        self,
        broker_url: AnyUrl,
        queue: str,
        origin: str,
        declare_queue: bool = False,
        allow_self_signed_certificates: bool = False,
    ):
        self._parameters = pika.URLParameters(str(broker_url))

        # Allow self-signed certificates if broker_url startswith 'ampqs'
        # and no ssl_options are present.
        if (
            str(broker_url).lower().startswith("amqps")
            and allow_self_signed_certificates
        ):
            context: ssl.SSLContext = self._parameters.ssl_options.context
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        self._queue = queue
        self._origin = origin
        self._declare_queue = declare_queue

    @sync_to_async
    def add(self, item: Json) -> Json:
        task = item["task"]
        args = list(item.get("args") or [])
        kwargs = dict(item.get("kwargs") or {})

        task_id = uuid.uuid4()
        header = CeleryHeaders(
            task=task,
            id=task_id,
            root_id=task_id,
            argsrepr=json.dumps(args),
            kwargsrepr=json.dumps(kwargs),
            origin=self._origin,
        )
        body = json.dumps((args, kwargs, None))

        with pika.BlockingConnection(self._parameters) as connection:
            channel = connection.channel()

            if self._declare_queue:
                channel.queue_declare(queue=self._queue)
            else:
                pass  # Configured by Lizard

            properties = pika.BasicProperties(
                correlation_id=str(task_id),
                content_type="application/json",
                content_encoding="utf-8",
                headers=header.json_dict(),
            )
            channel.basic_publish(
                exchange="",
                routing_key=self._queue,
                body=body,
                properties=properties,
            )

        return item
