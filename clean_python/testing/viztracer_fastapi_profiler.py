import random
import re
import threading
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel
from starlette.types import ASGIApp
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send
from viztracer import VizTracer

from clean_python import now


class VizTracerSettings(BaseModel):
    profile_dir: Path
    pattern: str = ".*"
    max_stack_depth: int = -1
    min_duration: float = 0  # in us
    fraction: float = 1.0


class VizTracerMiddleware:
    def __init__(self, app: ASGIApp, *, settings: VizTracerSettings):
        self.app = app
        self.enabled = True
        self.profile_dir = settings.profile_dir
        self.profile_dir.mkdir(exist_ok=True)
        self.path_pattern = re.compile(settings.pattern)
        self.fraction = settings.fraction
        self.tracer = VizTracer(
            log_async=True,
            ignore_c_function=True,
            min_duration=settings.min_duration,
            max_stack_depth=settings.max_stack_depth,
        )
        self.lock = threading.Lock()

    def should_profile(self, path: str) -> bool:
        return (
            self.enabled
            and self.path_pattern.match(path)
            and random.random() <= self.fraction
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and self.should_profile(scope["path"])
            and self.lock.acquire(blocking=False)
        ):
            try:
                self.tracer.start()
                begin = now()
                await self.app(scope, receive, send)
                end = now()
            finally:
                self.tracer.stop()
                self.tracer.save(self.file_name(scope["path"], begin, end).as_posix())
                self.tracer.clear()
                self.lock.release()
        else:
            await self.app(scope, receive, send)

    def file_name(self, path: str, begin: datetime, end: datetime):
        return (
            self.profile_dir
            / f"{path[1:].replace('/', '.')}-{int((end - begin).total_seconds() * 1000)}-{begin.isoformat()}.json"
        )
