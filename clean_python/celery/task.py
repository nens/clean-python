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


class BaseTask(Task):
    def apply_async(self, args=None, kwargs=None, **options):
        # include correlation_id and tenant in the headers
        if options.get("headers") is not None:
            headers = options["headers"].copy()
        else:
            headers = {}
        headers[HEADER_FIELD] = TaskHeaders(
            tenant=ctx.tenant, correlation_id=ctx.correlation_id
        ).model_dump(mode="json")
        return super().apply_async(args, kwargs, headers=headers, **options)

    def __call__(self, *args, **kwargs):
        return copy_context().run(self._call_with_context, *args, **kwargs)

    def _call_with_context(self, *args, **kwargs):
        if self.request.headers and HEADER_FIELD in self.request.headers:
            headers = TaskHeaders(**self.request.headers[HEADER_FIELD])
            ctx.tenant = headers.tenant
            ctx.correlation_id = headers.correlation_id
        if ctx.correlation_id is None:
            ctx.correlation_id = uuid4()
        return super().__call__(*args, **kwargs)
