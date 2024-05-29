from typing import Any

from ..domain import Json


class Mapper:
    def to_internal(self, external: Any) -> Json:
        return external

    def to_external(self, internal: Json) -> Json:
        return internal
