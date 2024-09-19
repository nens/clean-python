from clean_python.celery import CeleryConfig

from .tasks import sleep_task  # NOQA

app = CeleryConfig(
    broker_url="amqp://cleanpython:cleanpython@localhost/cleanpython",
    result_backend="rpc://",
).apply()
