# (c) Nelen & Schuurmans

import logging
from typing import Any

import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from pydantic import BaseModel
from pydantic import ValidationError

from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python import User

from .token import Token

__all__ = [
    "BaseTokenVerifier",
    "TokenVerifier",
    "NoAuthTokenVerifier",
    "TokenVerifierSettings",
    "OAuth2Settings",
]

logger = logging.getLogger(__name__)


class TokenVerifierSettings(BaseModel):
    issuer: str
    algorithms: list[str] = ["RS256"]
    # optional additional checks:
    scope: str | None = None
    admin_users: list[str] | None = None  # 'sub' whitelist
    jwks_timeout: float = 1.0


class OAuth2Settings(BaseModel):
    # this is primarily meant for documenting how OAuth2 works (in the schema)
    token_url: str = ""
    authorization_url: str = ""
    scopes: dict[str, str] = {}  # explanation of all possible scopes
    client_id: str | None = None  # when given, Swagger login function is enabled

    def login_enabled(self) -> bool:
        return bool(self.token_url and self.authorization_url and self.client_id)


class BaseTokenVerifier:
    def force(self, token: Token) -> None:
        raise NotImplementedError()

    def __call__(self, authorization: str | None) -> Token:
        raise NotImplementedError()


class NoAuthTokenVerifier(BaseTokenVerifier):
    def __init__(self):
        self.token = Token(
            claims={"sub": "DEV", "username": "dev", "scope": "superuser"}
        )

    def force(self, token: Token) -> None:
        self.token = token

    def __call__(self, authorization: str | None) -> Token:
        return self.token


class TokenVerifier(BaseTokenVerifier):
    """A class for verifying OAuth2 Access Tokens from AWS Cognito

    The verification steps followed are documented here:

    https://docs.aws.amazon.com/cognito/latest/developerguide/amazon- ⏎
    cognito-user-pools-using-tokens-verifying-a-jwt.html
    """

    # allow 2 minutes leeway for verifying token expiry:
    LEEWAY = 120

    def __init__(self, settings: TokenVerifierSettings):
        self.settings = settings
        self.jwk_client = PyJWKClient(
            f"{settings.issuer}/.well-known/jwks.json",
            timeout=self.settings.jwks_timeout,
        )

    def __call__(self, authorization: str | None) -> Token:
        # Step 0: retrieve the token from the Authorization header
        # See https://tools.ietf.org/html/rfc6750#section-2.1,
        # Bearer is case-sensitive and there is exactly 1 separator after.
        if authorization is None:
            raise Unauthorized("Missing Authorization header")
        jwt_str = authorization[7:] if authorization.startswith("Bearer") else None
        if jwt_str is None:
            raise Unauthorized("Authorization does not start with 'Bearer '")

        # Step 1: Confirm the structure of the JWT. This check is part of get_kid since
        # jwt.get_unverified_header will raise a JWTError if the structure is wrong.
        try:
            key = self.get_key(jwt_str)  # JSON Web Key
        except PyJWTError as e:
            raise Unauthorized(f"Token is invalid: {e}")
        # Step 2: Validate the JWT signature and standard claims
        try:
            claims = jwt.decode(
                jwt_str,
                key.key,
                algorithms=self.settings.algorithms,
                issuer=self.settings.issuer,
                leeway=self.LEEWAY,
                options={
                    "require": ["exp", "iss", "sub", "scope", "token_use"],
                },
            )
        except PyJWTError as e:
            raise Unauthorized(f"Token is invalid: {e}")
        # Step 3: Verify additional claims. At this point, we have passed
        # verification, so unverified claims may be used safely.
        self.verify_token_use(claims)
        try:
            token = Token(claims=claims)
        except ValidationError as e:
            raise Unauthorized(f"Token is invalid: {e}")
        self.verify_scope(token.scope)
        # Step 4: Authorization: verify user id ('sub' claim) against 'admin_users'
        self.authorize_user(token.user)
        return token

    def get_key(self, token: str) -> jwt.PyJWK:
        """Return the JSON Web KEY (JWK) corresponding to kid."""
        return self.jwk_client.get_signing_key_from_jwt(token)

    def verify_token_use(self, claims: dict[str, Any]) -> None:
        """Check the token_use claim."""
        if claims["token_use"] != "access":
            raise Unauthorized(
                f"Token has invalid token_use claim: {claims['token_use']}"
            )

    def verify_scope(self, claims_scope: frozenset[str]) -> None:
        """Parse scopes and optionally check scope claim."""
        if self.settings.scope is None:
            return
        if self.settings.scope not in claims_scope:
            raise Unauthorized(f"Token is missing '{self.settings.scope}' scope")

    def authorize_user(self, user: User) -> None:
        if self.settings.admin_users is None:
            return
        if user.id not in self.settings.admin_users:
            raise PermissionDenied()
