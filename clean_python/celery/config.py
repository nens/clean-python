from celery import Celery
from celery import current_app

from clean_python import Json
from clean_python import ValueObject
from clean_python.celery import BaseTask

__all__ = ["CeleryConfig"]


class CeleryConfig(ValueObject):
    timezone: str = "Europe/Amsterdam"
    broker_url: str
    broker_transport_options: Json = {"socket_timeout": 2}
    broker_connection_retry_on_startup: bool = True
    result_backend: str | None = None
    worker_prefetch_multiplier: int = 1
    task_always_eager: bool = False
    task_eager_propagates: bool = False
    task_acks_late: bool = True
    task_default_queue: str = "default"
    task_default_priority: int = 0
    task_queue_max_priority: int = 10
    task_track_started: bool = True

    def apply(self, strict_typing: bool = True) -> Celery:
        app = current_app if current_app else Celery()
        app.task_cls = BaseTask
        app.strict_typing = strict_typing
        app.config_from_object(self)

        # check if Sentry is configured in such a way that it would break clean-python
        try:
            import sentry_sdk
            from sentry_sdk.integrations.celery import CeleryIntegration

            integration = sentry_sdk.get_client().get_integration(CeleryIntegration)
            if integration is not None and integration.propagate_traces:
                raise ValueError(
                    "Sentry's trace propagation makes use of message headers in such a way that it prevents "
                    "clean-python to set the headers. "
                    "Please disable it as such: ... integrations=[CeleryIntegration(propagate_traces=False)]"
                )
        except ImportError:
            pass
        return app
