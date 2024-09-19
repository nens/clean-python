import os
from pathlib import Path

from clean_python.celery import CeleryConfig
from clean_python.celery import CeleryTaskLogger
from clean_python.celery import set_task_logger

from .logger import MultilineJsonFileGateway
from .tasks import sleep_task  # NOQA

app = CeleryConfig(
    broker_url="amqp://cleanpython:cleanpython@localhost/cleanpython",
    result_backend="rpc://",
).apply()
# the file path is set from the test fixture
logging_path = os.environ.get("CLEAN_PYTHON_TEST_LOGGING")
if logging_path:
    set_task_logger(CeleryTaskLogger(MultilineJsonFileGateway(Path(logging_path))))
