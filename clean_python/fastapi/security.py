from typing import Callable

from fastapi import Request
from fastapi.security import HTTPBearer
from fastapi.security import OAuth2AuthorizationCodeBearer
from fastapi.security import SecurityScopes

from clean_python import ctx
from clean_python import PermissionDenied
from clean_python import Scope
from clean_python.oauth2 import OAuth2Settings
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifier
from clean_python.oauth2 import TokenVerifierSettings

__all__ = ["get_token", "default_scope_verifier"]

# the scheme is stored globally enabling for the "get_token" callable
scheme: Callable[..., Token] | None = None


def set_auth_scheme(
    auth: TokenVerifierSettings | None,
    oauth2: OAuth2Settings | None,
    scope_verifier: Callable[[Token, Scope], None],
) -> Callable[..., Token] | None:
    global scheme

    if auth is None:
        scheme = None
    elif oauth2 is None:
        scheme = JWTBearerTokenSchema(auth, scope_verifier)
    else:
        scheme = OAuth2Schema(auth, oauth2, scope_verifier)

    return scheme


def get_token(request: Request) -> Token:
    """A fastapi 'dependable' yielding the validated token"""
    global scheme
    assert scheme is not None
    return scheme(request)


def default_scope_verifier(token: Token, endpoint_scopes: Scope) -> None:
    """Verifies whether any of the endpoint_scopes is in the token."""
    if not all(x in token.scope for x in endpoint_scopes):
        raise PermissionDenied(
            f"this operation requires '{' '.join(endpoint_scopes)}' scope"
        )


def _set_token_context(token: Token) -> None:
    ctx.user = token.user
    ctx.tenant = token.tenant


class OAuth2Schema(OAuth2AuthorizationCodeBearer):
    """A fastapi 'dependable' configuring the openapi schema for the
    OAuth2 Authorization Code Flow with PKCE extension.

    This includes the JWT Bearer token configuration.
    """

    def __init__(
        self,
        auth: TokenVerifierSettings,
        oauth2: OAuth2Settings,
        scope_verifier=Callable[[Token, list[Scope]], None],
    ):
        self._verifier = TokenVerifier(settings=auth)
        self._scope_verifier = scope_verifier
        super().__init__(
            scheme_name="OAuth2",
            authorizationUrl=str(oauth2.authorization_url),
            tokenUrl=str(oauth2.token_url),
            scopes=oauth2.scopes,
        )

    async def __call__(
        self, request: Request, security_scopes: SecurityScopes
    ) -> Token:
        token = self._verifier(request.headers.get("Authorization"))
        self._scope_verifier(token, security_scopes.scopes)
        _set_token_context(token)
        return token


class JWTBearerTokenSchema(HTTPBearer):
    """A fastapi 'dependable' configuring the openapi schema for JWT Bearer tokens."""

    def __init__(
        self,
        auth: TokenVerifierSettings,
        scope_verifier=Callable[[Token, list[Scope]], None],
    ):
        self._verifier = TokenVerifier(settings=auth)
        self._scope_verifier = scope_verifier
        super().__init__(scheme_name="Bearer", bearerFormat="JWT")

    async def __call__(
        self, request: Request, security_scopes: SecurityScopes
    ) -> Token:
        token = self._verifier(request.headers.get("Authorization"))
        self._scope_verifier(token, security_scopes.scopes)
        _set_token_context(token)
        return token
