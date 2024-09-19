import time

from celery import shared_task
from celery import Task
from celery.exceptions import Ignore
from celery.exceptions import Reject

from clean_python import ctx


@shared_task(bind=True, name="testing")
def sleep_task(self: Task, seconds: float, return_value=None, event="success"):
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
