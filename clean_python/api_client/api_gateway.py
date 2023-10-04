from datetime import datetime
from http import HTTPStatus
from typing import Optional

import inject

from clean_python import DoesNotExist
from clean_python import Gateway
from clean_python import Id
from clean_python import Json
from clean_python import Mapper
from clean_python import SyncGateway

from .api_provider import ApiProvider
from .exceptions import ApiException
from .sync_api_provider import SyncApiProvider

__all__ = ["ApiGateway", "SyncApiGateway"]


class ApiGateway(Gateway):
    path: str
    mapper = Mapper()

    def __init__(self, provider_override: Optional[ApiProvider] = None):
        self.provider_override = provider_override

    def __init_subclass__(cls, path: str) -> None:
        assert not path.startswith("/")
        assert "{id}" in path
        cls.path = path
        super().__init_subclass__()

    @property
    def provider(self) -> ApiProvider:
        return self.provider_override or inject.instance(ApiProvider)

    async def get(self, id: Id) -> Optional[Json]:
        try:
            result = await self.provider.request("GET", self.path.format(id=id))
            assert result is not None
            return self.mapper.to_internal(result)
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                return None
            raise e

    async def add(self, item: Json) -> Json:
        item = self.mapper.to_external(item)
        result = await self.provider.request("POST", self.path.format(id=""), json=item)
        assert result is not None
        return self.mapper.to_internal(result)

    async def remove(self, id: Id) -> bool:
        try:
            await self.provider.request("DELETE", self.path.format(id=id)) is not None
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                return False
            raise e
        else:
            return True

    async def update(
        self, item: Json, if_unmodified_since: Optional[datetime] = None
    ) -> Json:
        if if_unmodified_since is not None:
            raise NotImplementedError("if_unmodified_since not implemented")
        item = self.mapper.to_external(item)
        id_ = item.pop("id", None)
        if id_ is None:
            raise DoesNotExist("resource", id_)
        try:
            result = await self.provider.request(
                "PATCH", self.path.format(id=id_), json=item
            )
            assert result is not None
            return self.mapper.to_internal(result)
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                raise DoesNotExist("resource", id_)
            raise e


# This is a copy-paste of ApiGateway:


class SyncApiGateway(SyncGateway):
    path: str
    mapper = Mapper()

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
            result = self.provider.request("GET", self.path.format(id=id))
            assert result is not None
            return self.mapper.to_internal(result)
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                return None
            raise e

    def add(self, item: Json) -> Json:
        item = self.mapper.to_external(item)
        result = self.provider.request("POST", self.path.format(id=""), json=item)
        assert result is not None
        return self.mapper.to_internal(result)

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
        item = self.mapper.to_external(item)
        id_ = item.pop("id", None)
        if id_ is None:
            raise DoesNotExist("resource", id_)
        try:
            result = self.provider.request("PATCH", self.path.format(id=id_), json=item)
            assert result is not None
            return self.mapper.to_internal(result)
        except ApiException as e:
            if e.status is HTTPStatus.NOT_FOUND:
                raise DoesNotExist("resource", id_)
            raise e
