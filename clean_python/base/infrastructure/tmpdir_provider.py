# (c) Nelen & Schuurmans

from tempfile import TemporaryDirectory

from .provider import SyncProvider

__all__ = ["TmpDirProvider"]


class TmpDirProvider(SyncProvider):
    def __init__(self, dir: str | None = None):
        self.dir = dir

    def __call__(self) -> TemporaryDirectory:  # type: ignore
        return TemporaryDirectory(dir=self.dir)
