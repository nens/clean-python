import json
from pathlib import Path

from clean_python import Filter
from clean_python import Json
from clean_python import PageOptions
from clean_python import SyncGateway

__all__ = ["MultilineJsonFileGateway"]


class MultilineJsonFileGateway(SyncGateway):
    def __init__(self, path: Path) -> None:
        self.path = path

    def clear(self):
        if self.path.exists():
            self.path.unlink()

    def filter(
        self, filters: list[Filter], params: PageOptions | None = None
    ) -> list[Json]:
        assert not filters
        assert not params
        if not self.path.exists():
            return []
        with self.path.open("r") as f:
            return [json.loads(line) for line in f]

    def add(self, item: Json) -> Json:
        with self.path.open("a") as f:
            f.write(json.dumps(item))
            f.write("\n")
        return item
