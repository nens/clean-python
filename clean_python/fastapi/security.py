from typing import Optional

from fastapi import Depends
from fastapi import Request
from fastapi.security import HTTPBearer
from fastapi.security import OAuth2AuthorizationCodeBearer

from clean_python import PermissionDenied
from clean_python import Tenant
from clean_python import User
from clean_python.oauth2 import BaseTokenVerifier
from clean_python.oauth2 import NoAuthTokenVerifier
from clean_python.oauth2 import OAuth2SPAClientSettings
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifier
from clean_python.oauth2 import TokenVerifierSettings

__all__ = [
    "get_token",
    "get_user",
    "get_tenant",
    "RequiresScope",
]

verifier: Optional[BaseTokenVerifier] = None


def clear_verifier() -> None:
    global verifier

    verifier = None


def set_verifier(settings: Optional[TokenVerifierSettings]) -> None:
    global verifier

    if settings is None:
        verifier = NoAuthTokenVerifier()
    else:
        verifier = TokenVerifier(settings=settings)


def get_token(request: Request) -> Token:
    """A fastapi 'dependable' yielding the validated token"""
    global verifier

    assert verifier is not None
    return verifier(request.headers.get("Authorization"))


async def get_user(token: Token = Depends(get_token)) -> User:
    return token.user


async def get_tenant(token: Token = Depends(get_token)) -> Tenant:
    if token.tenant is None:
        raise PermissionDenied("this operation requires a tenant-scoped token")
    return token.tenant


class RequiresScope:
    def __init__(self, scope: str):
        assert scope.replace(" ", "") == scope, "spaces are not allowed in a scope"
        self.scope = scope

    async def __call__(self, token: Token = Depends(get_token)) -> None:
        if self.scope not in token.scope:
            raise PermissionDenied(f"this operation requires '{self.scope}' scope")


class OAuth2SPAClientSchema(OAuth2AuthorizationCodeBearer):
    """A fastapi 'dependable' configuring the openapi schema for the
    OAuth2 Authorization Code Flow with PKCE extension.

    This includes the JWT Bearer token configuration.
    """

    def __init__(self, client: OAuth2SPAClientSettings):
        super().__init__(
            scheme_name="OAuth2 Authorization Code Flow with PKCE",
            authorizationUrl=str(client.authorization_url),
            tokenUrl=str(client.token_url),
        )

    async def __call__(self, token: Optional[Token] = Depends(get_token)) -> None:
        pass


class JWTBearerTokenSchema(HTTPBearer):
    """A fastapi 'dependable' configuring the openapi schema for JWT Bearer tokens.

    Note: for the client-side OAuth2 flow, use OAuth2SPAClientSchema instead.
    """

    def __init__(self):
        super().__init__(scheme_name="JWT Bearer token", bearerFormat="JWT")

    async def __call__(self, token: Optional[Token] = Depends(get_token)) -> None:
        pass
