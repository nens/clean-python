from typing import Annotated
from typing import FrozenSet
from typing import Optional

from fastapi import Depends
from fastapi import Request
from fastapi.security import HTTPBearer
from fastapi.security import OAuth2AuthorizationCodeBearer

from clean_python.oauth2 import Claims
from clean_python.oauth2 import OAuth2SPAClientSettings
from clean_python.oauth2 import Tenant
from clean_python.oauth2 import TokenVerifier
from clean_python.oauth2 import TokenVerifierSettings
from clean_python.oauth2 import User

__all__ = ["get_verified_claims", "get_user", "get_tenant", "get_scope"]

verifier: Optional[TokenVerifier] = None


def clear_verifier() -> None:
    global verifier

    verifier = None


def set_verifier(settings: TokenVerifierSettings) -> None:
    global verifier

    verifier = TokenVerifier(settings=settings)


def get_verified_claims(request: Request) -> Optional[Claims]:
    """A fastapi 'dependable' yielding the validated token Claims"""
    global verifier

    if verifier is None:
        return None

    return verifier(request.headers.get("Authorization"))


async def get_user(
    claims: Annotated[Optional[Claims], Depends(get_verified_claims)]
) -> Optional[User]:
    if claims is None:
        return None

    return claims.user


async def get_tenant(
    claims: Annotated[Optional[Claims], Depends(get_verified_claims)]
) -> Optional[Tenant]:
    if claims is None:
        return None

    return claims.tenant


async def get_scope(
    claims: Annotated[Optional[Claims], Depends(get_verified_claims)]
) -> Optional[FrozenSet[str]]:
    if claims is None:
        return None

    return claims.scope


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

    async def __call__(
        self, claims: Annotated[Optional[Claims], Depends(get_verified_claims)]
    ) -> None:
        pass


class JWTBearerTokenSchema(HTTPBearer):
    """A fastapi 'dependable' configuring the openapi schema for JWT Bearer tokens.

    Note: for the client-side OAuth2 flow, use OAuth2SPAClientSchema instead.
    """

    def __init__(self):
        super().__init__(scheme_name="JWT Bearer token", bearerFormat="JWT")

    async def __call__(
        self, claims: Annotated[Optional[Claims], Depends(get_verified_claims)]
    ) -> None:
        pass
