# (c) Nelen & Schuurmans

from tempfile import TemporaryDirectory

__all__ = ["TmpDirProvider"]


class TmpDirProvider:
    def __init__(self, dir: str | None = None):
        self.dir = dir

    def __call__(self) -> TemporaryDirectory:  # type: ignore
        return TemporaryDirectory(dir=self.dir)
