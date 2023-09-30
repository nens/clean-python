from datetime import datetime
from http import HTTPStatus
from typing import Optional

import inject

from clean_python import DoesNotExist
from clean_python import Id
from clean_python import Json

from .. import SyncGateway
from .api_provider import SyncApiProvider
from .exceptions import ApiException

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
        try:
            return self.provider.request("GET", self.path.format(id=id))
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                return None
            raise e

    def add(self, item: Json) -> Json:
        result = self.provider.request("POST", self.path.format(id=""), json=item)
        assert result is not None
        return result

    def remove(self, id: Id) -> bool:
        try:
            self.provider.request("DELETE", self.path.format(id=id)) is not None
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                return False
            raise e
        else:
            return True

    def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        if if_unmodified_since is not None:
            raise NotImplementedError("if_unmodified_since not implemented")
        item = item.copy()
        id_ = item.pop("id", None)
        if id_ is None:
            raise DoesNotExist("resource", id_)
        try:
            result = self.provider.request("PATCH", self.path.format(id=id_), json=item)
            assert result is not None
            return result
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                raise DoesNotExist("resource", id_)
            raise e
