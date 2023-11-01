# (c) Nelen & Schuurmans

import os
import threading
import time
from typing import Optional
from uuid import UUID
from uuid import uuid4

import inject
from dramatiq import get_encoder
from dramatiq import Message
from dramatiq import Middleware
from dramatiq.errors import RateLimitExceeded
from dramatiq.errors import Retry
from dramatiq.middleware import SkipMessage

from clean_python import ctx
from clean_python import Gateway
from clean_python.fluentbit import FluentbitGateway

__all__ = ["AsyncLoggingMiddleware", "DramatiqTaskLogger"]


class AsyncLoggingMiddleware(Middleware):
    def __init__(self, **kwargs):
        self.logger = DramatiqTaskLogger(**kwargs)

    def before_enqueue(self, broker, message: Message, delay):
        if ctx.correlation_id is not None:
            message.options["correlation_id"] = str(ctx.correlation_id)

    def before_process_message(self, broker, message):
        if message.options.get("correlation_id") is not None:
            ctx.correlation_id = UUID(message.options["correlation_id"])
        else:
            ctx.correlation_id = uuid4()
        broker.run_coroutine(self.logger.start())

    def after_skip_message(self, broker, message):
        broker.run_coroutine(self.logger.stop(message, None, SkipMessage()))
        ctx.correlation_id = None

    def after_process_message(self, broker, message, *, result=None, exception=None):
        broker.run_coroutine(self.logger.stop(message, result, exception))
        ctx.correlation_id = None


class DramatiqTaskLogger:
    local = threading.local()

    def __init__(
        self,
        hostname: str,
        gateway_override: Optional[Gateway] = None,
    ):
        self.origin = f"{hostname}-{os.getpid()}"
        self.gateway_override = gateway_override

    @property
    def gateway(self):
        return self.gateway_override or inject.instance(FluentbitGateway)

    @property
    def encoder(self):
        return get_encoder()

    async def start(self):
        self.local.start_time = time.time()

    async def stop(self, message: Message, result=None, exception=None):
        if exception is None:
            state = "SUCCESS"
        elif isinstance(exception, Retry):
            state = "RETRY"
        elif isinstance(exception, SkipMessage):
            state = "EXPIRED"
        elif isinstance(exception, RateLimitExceeded):
            state = "TERMINATED"
        else:
            state = "FAILURE"

        try:
            duration = time.time() - self.local.start_time
        except AttributeError:
            duration = 0

        try:
            start_time = self.local.start_time
        except AttributeError:
            start_time = None

        log_dict = {
            "tag_suffix": "task_log",
            "task_id": message.message_id,
            "name": message.actor_name,
            "state": state,
            "duration": duration,
            "retries": message.options.get("retries", 0),
            "origin": self.origin,
            "argsrepr": self.encoder.encode(message.args),
            "kwargsrepr": self.encoder.encode(message.kwargs),
            "result": result,
            "time": start_time,
            "correlation_id": str(ctx.correlation_id) if ctx.correlation_id else None,
        }
        return await self.gateway.add(log_dict)
