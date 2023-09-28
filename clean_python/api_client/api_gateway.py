from typing import Optional

import inject

from clean_python import Id
from clean_python import Json

from .. import SyncGateway
from .api_provider import SyncApiProvider

__all__ = ["SyncApiGateway"]


class SyncApiGateway(SyncGateway):
    path: str

    def __init__(self, provider_override: Optional[SyncApiProvider] = None):
        self.provider_override = provider_override

    def __init_subclass__(cls, path: str) -> None:
        assert not path.startswith("/")
        assert "{id}" in path
        cls.path = path
        super().__init_subclass__()

    @property
    def provider(self) -> SyncApiProvider:
        return self.provider_override or inject.instance(SyncApiProvider)

    def get(self, id: Id) -> Optional[Json]:
        return self.provider.request("GET", self.path.format(id=id))

    def add(self, item: Json) -> Json:
        result = self.provider.request("POST", self.path.format(id=""), json=item)
        assert result is not None
        return result

    def remove(self, id: Id) -> bool:
        return self.provider.request("DELETE", self.path.format(id=id)) is not None
