import time

from celery import Celery
from celery.exceptions import Ignore
from celery.exceptions import Reject

from clean_python import ctx
from clean_python.celery import BaseTask

app = Celery(
    "tasks",
    strict_typing=False,
    backend="rpc://",
    broker="amqp://cleanpython:cleanpython@localhost/cleanpython",
)
app.conf.update(
    task_cls=BaseTask,
    strict_typing=False,
    broker_transport_options={"socket_timeout": 2},
    timezone="Europe/Amsterdam",
    task_default_priority=0,
    task_queue_max_priority=10,
    task_track_started=True,
    broker_connection_retry_on_startup=True,  # default, but hides deprecationwarning
)


@app.task(bind=True, base=BaseTask, name="testing")
def sleep_task(self: BaseTask, seconds: float, return_value=None, event="success"):
    event = event.lower()
    if event == "success":
        time.sleep(int(seconds))
    elif event == "crash":
        import ctypes

        ctypes.string_at(0)  # segfault
    elif event == "ignore":
        raise Ignore()
    elif event == "reject":
        raise Reject()
    elif event == "retry":
        raise self.retry(countdown=seconds, max_retries=1)
    elif event == "context":
        return {
            "tenant_id": ctx.tenant.id,
            "correlation_id": str(ctx.correlation_id),
        }
    else:
        raise ValueError(f"Unknown event '{event}'")

    return {"value": return_value}
