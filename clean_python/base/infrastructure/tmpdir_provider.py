# (c) Nelen & Schuurmans

from tempfile import TemporaryDirectory
from typing import Optional

__all__ = ["TmpDirProvider"]


class TmpDirProvider:
    def __init__(self, dir: Optional[str] = None):
        self.dir = dir

    def __call__(self) -> TemporaryDirectory:  # type: ignore
        return TemporaryDirectory(dir=self.dir)
