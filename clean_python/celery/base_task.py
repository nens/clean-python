from contextvars import copy_context
from typing import Optional
from typing import Tuple
from uuid import UUID
from uuid import uuid4

from celery import Task

from clean_python import ctx
from clean_python import Json
from clean_python import Tenant
from clean_python import ValueObject

__all__ = ["BaseTask"]


HEADER_FIELD = "clean_python_context"


class TaskHeaders(ValueObject):
    tenant: Optional[Tenant]
    correlation_id: Optional[UUID]

    @classmethod
    def from_kwargs(cls, kwargs: Json) -> Tuple["TaskHeaders", Json]:
        if HEADER_FIELD in kwargs:
            kwargs = kwargs.copy()
            headers = kwargs.pop(HEADER_FIELD)
            return TaskHeaders(**headers), kwargs
        else:
            return TaskHeaders(tenant=None, correlation_id=None), kwargs


class BaseTask(Task):
    def apply_async(self, args=None, kwargs=None, **options):
        # include correlation_id and tenant in the kwargs
        # and NOT the headers as that is buggy in celery
        # see  https://github.com/celery/celery/issues/4875
        kwargs = {} if kwargs is None else kwargs.copy()
        kwargs[HEADER_FIELD] = TaskHeaders(
            tenant=ctx.tenant, correlation_id=ctx.correlation_id or uuid4()
        ).model_dump(mode="json")
        return super().apply_async(args, kwargs, **options)

    def __call__(self, *args, **kwargs):
        return copy_context().run(self._call_with_context, *args, **kwargs)

    def _call_with_context(self, *args, **kwargs):
        headers, kwargs = TaskHeaders.from_kwargs(kwargs)
        ctx.tenant = headers.tenant
        ctx.correlation_id = headers.correlation_id
        return super().__call__(*args, **kwargs)
