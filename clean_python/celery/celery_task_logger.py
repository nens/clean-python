# (c) Nelen & Schuurmans

import json
import threading
import time
from typing import Any
from typing import Optional

import inject
from billiard.einfo import ExceptionInfo
from celery import Task
from celery.signals import task_failure
from celery.signals import task_postrun
from celery.signals import task_prerun
from celery.signals import task_revoked
from celery.signals import task_success
from celery.states import FAILURE
from celery.states import RETRY
from celery.states import REVOKED
from celery.states import SUCCESS

from clean_python import SyncGateway
from clean_python.fluentbit import SyncFluentbitGateway

from .base_task import TaskHeaders

__all__ = ["CeleryTaskLogger", "set_task_logger"]


class CeleryTaskLogger:
    local = threading.local()

    def __init__(self, gateway_override: Optional[SyncGateway] = None):
        self.gateway_override = gateway_override

    @property
    def gateway(self) -> SyncGateway:
        return self.gateway_override or inject.instance(SyncFluentbitGateway)

    def start(self):
        self.local.start_time = time.time()

    def stop(self, task: Task, state: str, result: Any = None):
        # format the result into a dict (elasticsearch will error otherwise)
        if result is not None and not isinstance(result, dict):
            result = {"result": result}
        try:
            result_json = json.loads(json.dumps(result))
        except TypeError:
            result_json = None

        try:
            start_time = self.local.start_time
        except AttributeError:
            start_time = None

        self.local.start_time = None

        if start_time is not None:
            duration = time.time() - start_time
        else:
            duration = None

        try:
            request = task.request
            correlation_id = TaskHeaders.from_celery_request(request).correlation_id
        except AttributeError:
            request = None
            correlation_id = None

        log_dict = {
            "tag_suffix": "task_log",
            "time": start_time,
            "task_id": getattr(request, "id", None),
            "name": task.name,
            "state": state,
            "duration": duration,
            "origin": getattr(request, "origin", None),
            "retries": getattr(request, "retries", None),
            "argsrepr": getattr(request, "argsrepr", None),
            "kwargsrepr": getattr(request, "kwargsrepr", None),
            "result": result_json,
            "correlation_id": str(correlation_id) if correlation_id else None,
        }

        return self.gateway.add(log_dict)


celery_logger: Optional[CeleryTaskLogger] = None


def set_task_logger(logger: Optional[CeleryTaskLogger]):
    global celery_logger
    celery_logger = logger


@task_prerun.connect
def task_prerun_log(**kwargs):
    if celery_logger is None:
        return
    celery_logger.start()


@task_postrun.connect
def task_postrun_log(sender: Task, state: str, **kwargs):
    if celery_logger is None:
        return
    if state not in {None, SUCCESS, FAILURE, RETRY}:
        celery_logger.stop(task=sender, state=state)


@task_success.connect
def task_success_log(sender: Task, result: Any, **kwargs):
    if celery_logger is None:
        return
    celery_logger.stop(task=sender, state=SUCCESS, result=result)


@task_failure.connect
def task_failure_log(sender: Task, einfo: ExceptionInfo, **kwargs):
    if celery_logger is None:
        return
    celery_logger.stop(
        task=sender, state=FAILURE, result={"traceback": einfo.traceback}
    )


@task_revoked.connect(dispatch_uid="task_revoked_log")
def task_revoked_log(sender: Task, **kwargs):
    if celery_logger is None:
        return
    if str(kwargs["signum"]) == "Signals.SIGTERM":
        # This to filter out duplicate logging on task termination.
        return
    if kwargs["terminated"]:
        state = "TERMINATED"
    elif kwargs["expired"]:
        state = "EXPIRED"
    else:
        state = REVOKED
    celery_logger.stop(task=sender, state=state)
