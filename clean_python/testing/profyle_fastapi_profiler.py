import random
import sqlite3
import threading
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

try:
    from profyle.application.profyle import profyle
    from profyle.infrastructure.sqlite3.repository import SQLiteTraceRepository
except ImportError:
    profyle = SQLiteTraceRepository = None

from starlette.types import ASGIApp
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send


class ProfyleSettings(BaseModel):
    db_path: Path
    pattern: Optional[str] = None
    max_stack_depth: int = -1
    min_duration: int = 0
    fraction: float = 1.0


class ProfyleMiddleware:
    # adapted from https://github.com/vpcarlos/profyle/blob/main/profyle/infrastructure/middleware/fastapi.py
    # - added db_path parameter to adjust sqlite file location
    # - added fraction parameter for load testing
    # - moved settings into a BaseModel
    # to display profiles
    # - install profyle: "pip install profyle"
    # - find where profyle expects the sqlite: python -c "from profyle.settings import settings; print(settings.get_path('profile.db'))"
    # - symlink: ln -s {abspath to db file} {whatever previous step returned}
    def __init__(self, app: ASGIApp, *, settings: ProfyleSettings):
        print("initializing middleware..e")
        assert profyle is not None, "'pip install profyle' for using ProfyleMiddleware"
        self.app = app
        self.enabled = True
        self.pattern = settings.pattern
        self.max_stack_depth = settings.max_stack_depth
        self.min_duration = settings.min_duration
        self.fraction = settings.fraction
        self.trace_repo = SQLiteTraceRepository(
            sqlite3.connect(str(settings.db_path), check_same_thread=False)
        )
        self.lock = threading.Lock()
        # self.lock.release()

    def should_profile(self) -> bool:
        return self.enabled and random.random() <= self.fraction

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] == "http"
            and self.should_profile()
            and self.lock.acquire(blocking=False)
        ):
            try:
                method = scope.get("method", "").upper()
                path = scope.get("raw_path", b"").decode("utf-8")
                with profyle(
                    name=f"{method} {path}",
                    pattern=self.pattern,
                    repo=self.trace_repo,
                    max_stack_depth=self.max_stack_depth,
                    min_duration=self.min_duration,
                ):
                    await self.app(scope, receive, send)
                return
            finally:
                self.lock.release()
        await self.app(scope, receive, send)
