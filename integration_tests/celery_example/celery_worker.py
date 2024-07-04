import logging

from celery import Celery

from . import tasks  # NOQA
from .shared import BROKER_URL

logger = logging.getLogger()

app = Celery("tasks", broker=BROKER_URL, strict_typing=False)
app.conf.update(
    broker_connection_retry_on_startup=True,
    task_default_queue="default",
)
