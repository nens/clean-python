"""This is the glue between events and celery (RPC)"""
import fnmatch

import pika
from celery import Celery
from shared import BROKER_URL
from shared import EXCHANGE
from tasks import task_registry

RELAY_QUEUE_NAME = "celery_relay_queue"


app = Celery("tasks", broker=BROKER_URL, strict_typing=False)
app.conf.update(
    broker_connection_retry_on_startup=True,
    task_default_queue="default",
)


def on_message(chan, method_frame, header_frame, body):
    """Called when a message is received. Schedules the correct celery tasks."""
    print(f"Received {method_frame.routing_key}")
    for task, topic in task_registry.items():
        if fnmatch.fnmatch(method_frame.routing_key, topic):
            print(f"Relaying {method_frame.routing_key} to {task}")
            task.delay(body.decode())
    chan.basic_ack(delivery_tag=method_frame.delivery_tag)


def main():
    """Main method."""
    connection = pika.BlockingConnection(pika.URLParameters(BROKER_URL))

    channel = connection.channel()

    channel.queue_declare(queue=RELAY_QUEUE_NAME, durable=True)
    for routing_key in set(task_registry.values()):
        channel.queue_bind(
            queue=RELAY_QUEUE_NAME, exchange=EXCHANGE, routing_key=routing_key
        )
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(RELAY_QUEUE_NAME, on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

    connection.close()


if __name__ == "__main__":
    main()
