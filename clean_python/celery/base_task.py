from contextvars import copy_context
from uuid import UUID
from uuid import uuid4

from celery import Task
from celery.worker.request import Request as CeleryRequest

from clean_python import ctx
from clean_python import Id
from clean_python import Tenant
from clean_python import ValueObject

__all__ = ["BaseTask"]


class TaskHeaders(ValueObject):
    tenant_id: Id | None = None
    # avoid conflict with celery's own correlation_id:
    x_correlation_id: UUID | None = None

    @classmethod
    def from_celery_request(cls, request: CeleryRequest) -> "TaskHeaders":
        return cls(**request.headers)


class BaseTask(Task):
    def apply_async(self, args=None, kwargs=None, **options):
        # see  https://github.com/celery/celery/issues/4875
        options["headers"] = TaskHeaders(
            tenant_id=ctx.tenant.id if ctx.tenant else None,
            x_correlation_id=ctx.correlation_id or uuid4(),
        ).model_dump(mode="json")
        return super().apply_async(args, kwargs, **options)

    def __call__(self, *args, **kwargs):
        return copy_context().run(self._call_with_context, *args, **kwargs)

    def _call_with_context(self, *args, **kwargs):
        headers = TaskHeaders.from_celery_request(self.request)
        ctx.tenant = (
            Tenant(id=headers.tenant_id, name="") if headers.tenant_id else None
        )
        ctx.correlation_id = headers.x_correlation_id or uuid4()
        return super().__call__(*args, **kwargs)
