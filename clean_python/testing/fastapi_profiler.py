import re
from pathlib import Path

import yappi
from starlette.types import ASGIApp
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from clean_python import now


class ProfilerMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        profile_dir: Path,
        path: str = ".*",
    ):
        self.app = app
        profile_dir.mkdir(exist_ok=True)
        self.profile_dir = profile_dir
        self.path = re.compile(path)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.path.match(scope["path"]):
            await self.app(scope, receive, send)
            return

        yappi.set_clock_type("wall")
        received = now()
        with yappi.run():
            await self.app(scope, receive, send)
        stats = yappi.convert2pstats(yappi.get_func_stats())
        stats.dump_stats(self.profile_dir / f"{received.isoformat()}.pstats")
        yappi.clear_stats()
