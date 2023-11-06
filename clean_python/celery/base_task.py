from contextvars import copy_context
from uuid import UUID
from uuid import uuid4

from celery import Task

from clean_python import ctx
from clean_python import Tenant
from clean_python import ValueObject

__all__ = ["BaseTask"]


HEADER_FIELD = "clean_python_context"


class TaskHeaders(ValueObject):
    tenant: Tenant | None
    correlation_id: UUID | None

    @classmethod
    def from_celery_request(cls, request) -> "TaskHeaders":
        if request.headers and HEADER_FIELD in request.headers:
            return TaskHeaders(**request.headers[HEADER_FIELD])
        else:
            return TaskHeaders(tenant=None, correlation_id=None)


class BaseTask(Task):
    def apply_async(self, args=None, kwargs=None, **options):
        # include correlation_id and tenant in the headers
        if options.get("headers") is not None:
            headers = options["headers"].copy()
        else:
            headers = {}
        headers[HEADER_FIELD] = TaskHeaders(
            tenant=ctx.tenant, correlation_id=ctx.correlation_id or uuid4()
        ).model_dump(mode="json")
        return super().apply_async(args, kwargs, headers=headers, **options)

    def __call__(self, *args, **kwargs):
        return copy_context().run(self._call_with_context, *args, **kwargs)

    def _call_with_context(self, *args, **kwargs):
        headers = TaskHeaders.from_celery_request(self.request)
        ctx.tenant = headers.tenant
        ctx.correlation_id = headers.correlation_id
        return super().__call__(*args, **kwargs)
