import base64
import json
import time
from functools import lru_cache

from aiohttp import BasicAuth
from async_lru import alru_cache
from pydantic import AnyHttpUrl
from pydantic import BaseModel

from clean_python.api_client import ApiProvider
from clean_python.api_client import SyncApiProvider

__all__ = ["CCTokenGateway", "SyncCCTokenGateway", "OAuth2CCSettings"]


REFRESH_TIME_DELTA = 5 * 60  # in seconds


def decode_jwt(token):
    """Decode a JWT without checking its signature"""
    # JWT consists of {header}.{payload}.{signature}
    _, payload, _ = token.split(".")
    # JWT should be padded with = (base64.b64decode expects this)
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.b64decode(payload))


def is_token_usable(token: str, leeway: int) -> bool:
    """Determine whether the token has expired"""
    try:
        claims = decode_jwt(token)
    except Exception:
        return False

    exp = claims["exp"]
    refresh_on = exp - leeway
    return refresh_on >= int(time.time())


def get_auth_headers(client_id: str, client_secret: str) -> dict[str, str]:
    return {"Authorization": BasicAuth(client_id, client_secret).encode()}


class OAuth2CCSettings(BaseModel):
    token_url: AnyHttpUrl
    client_id: str
    client_secret: str
    scope: str
    timeout: float = 1.0  # in seconds
    leeway: int = 5 * 60  # in seconds


class CCTokenGateway:
    def __init__(self, settings: OAuth2CCSettings):
        self.scope = settings.scope
        self.timeout = settings.timeout
        self.leeway = settings.leeway

        auth_headers = get_auth_headers(settings.client_id, settings.client_secret)

        async def headers_factory():
            return auth_headers

        self.provider = ApiProvider(
            url=settings.token_url, headers_factory=headers_factory
        )
        # This binds the cache to the CCTokenGateway instance (and not the class)
        self.cached_fetch_token = alru_cache(self._fetch_token)

    async def _fetch_token(self) -> str:
        response = await self.provider.request(
            method="POST",
            path="",
            fields={"grant_type": "client_credentials", "scope": self.scope},
            timeout=self.timeout,
        )
        assert response is not None
        return response["access_token"]

    async def fetch_token(self) -> str:
        token_str = await self.cached_fetch_token()
        if not is_token_usable(token_str, self.leeway):
            self.cached_fetch_token.cache_clear()
            token_str = await self.cached_fetch_token()
        return token_str

    async def fetch_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self.fetch_token()}"}


# Copy-paste of async version:


class SyncCCTokenGateway:
    def __init__(self, settings: OAuth2CCSettings):
        self.scope = settings.scope
        self.timeout = settings.timeout
        self.leeway = settings.leeway

        auth_headers = get_auth_headers(settings.client_id, settings.client_secret)

        self.provider = SyncApiProvider(
            url=settings.token_url, headers_factory=lambda: auth_headers
        )
        # This binds the cache to the SyncCCTokenGateway instance (and not the class)
        self.cached_fetch_token = lru_cache(self._fetch_token)

    def _fetch_token(self) -> str:
        response = self.provider.request(
            method="POST",
            path="",
            fields={"grant_type": "client_credentials", "scope": self.scope},
            timeout=self.timeout,
        )
        assert response is not None
        return response["access_token"]

    def fetch_token(self) -> str:
        token_str = self.cached_fetch_token()
        if not is_token_usable(token_str, self.leeway):
            self.cached_fetch_token.cache_clear()
            token_str = self.cached_fetch_token()
        return token_str

    def fetch_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.fetch_token()}"}
