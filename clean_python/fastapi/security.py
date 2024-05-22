from typing import Callable

from fastapi import Request
from fastapi.security import OAuth2AuthorizationCodeBearer
from fastapi.security import SecurityScopes

from clean_python import ctx
from clean_python import PermissionDenied
from clean_python import Scope
from clean_python import ValueObject
from clean_python.oauth2 import OAuth2Settings
from clean_python.oauth2 import Token
from clean_python.oauth2 import TokenVerifier
from clean_python.oauth2 import TokenVerifierSettings

__all__ = ["get_token", "default_scope_verifier", "AuthSettings"]

ScopeVerifier = Callable[[Request, Scope, Token], None]


class OAuth2Schema(OAuth2AuthorizationCodeBearer):
    """A fastapi 'dependable' to verify bearer tokens obtained through OAuth2.

    This verification includes authentication and scope verification.

    Because this class derives from a FastAPI built in, the openapi schema for the
    OAuth2 Authorization Code Flow is automatically configured.
    """

    def __init__(self, settings: "AuthSettings"):
        self._verifier = TokenVerifier(settings.token)
        self._scope_verifier = settings.scope_verifier
        super().__init__(
            scheme_name="OAuth2",
            authorizationUrl=settings.oauth2.authorization_url,
            tokenUrl=settings.oauth2.token_url,
            scopes=settings.oauth2.scopes,
        )

    async def __call__(
        self, request: Request, security_scopes: SecurityScopes
    ) -> Token:
        """FastAPI magically fills these parameters:

        - 'request' with the current request
        - 'security_scopes' with the union of all Security(..., scopes) in use for
          the current operation. Note that this also automatically configures the
          "security requirements" in the openapi spec for this operation.
        """
        token = self._verifier(request.headers.get("Authorization"))
        self._scope_verifier(request, frozenset(security_scopes.scopes), token)
        ctx.user = token.user
        ctx.tenant = token.tenant
        return token


# the scheme is stored globally enabling for the "get_token" callable
scheme: OAuth2Schema | None = None


async def get_token(request: Request) -> Token:
    """A fastapi 'dependable' yielding the validated token"""
    global scheme
    assert scheme is not None
    return await scheme(request, SecurityScopes())


def default_scope_verifier(
    request: Request, endpoint_scopes: Scope, token: Token
) -> None:
    """Verifies whether any of the endpoint_scopes is in the token."""
    if not all(x in token.scope for x in endpoint_scopes):
        raise PermissionDenied(
            f"this operation requires '{' '.join(endpoint_scopes)}' scope"
        )


class AuthSettings(ValueObject):
    token: TokenVerifierSettings
    oauth2: OAuth2Settings = OAuth2Settings()
    scope_verifier: ScopeVerifier = default_scope_verifier


def set_auth_scheme(settings: AuthSettings | None) -> OAuth2Schema | None:
    global scheme

    if settings is not None:
        scheme = OAuth2Schema(settings)

    return scheme
