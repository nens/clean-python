from typing import Literal

import pika
from pika.exchange_type import ExchangeType

from clean_python import ValueObject

from .shared import BROKER_URL
from .shared import EXCHANGE

connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))
main_channel = connection.channel()
main_channel.exchange_declare(
    exchange=EXCHANGE, exchange_type=ExchangeType.topic, durable=True
)


class Event(ValueObject):
    topic: str

    def emit(self):
        main_channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=self.topic,
            body=self.model_dump_json(),
            properties=pika.BasicProperties(content_type="application/json"),
        )


class DatasetCreated(Event):
    topic: Literal["dataset.created"] = "dataset.created"
    id: str


class ProjectCreated(Event):
    topic: Literal["project.created"] = "project.created"
    id: str
