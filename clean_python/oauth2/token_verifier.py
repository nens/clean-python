# (c) Nelen & Schuurmans

import logging
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional

import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from pydantic import AnyHttpUrl
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
    "OAuth2SPAClientSettings",
]

logger = logging.getLogger(__name__)


class TokenVerifierSettings(BaseModel):
    issuer: str
    algorithms: List[str] = ["RS256"]
    # optional additional checks:
    scope: Optional[str] = None
    admin_users: Optional[List[str]] = None  # 'sub' whitelist


class OAuth2SPAClientSettings(BaseModel):
    client_id: str
    token_url: AnyHttpUrl
    authorization_url: AnyHttpUrl


class BaseTokenVerifier:
    def force(self, token: Token) -> None:
        raise NotImplementedError()

    def __call__(self, authorization: Optional[str]) -> Token:
        raise NotImplementedError()


class NoAuthTokenVerifier(BaseTokenVerifier):
    def __init__(self):
        self.token = Token(
            claims={"sub": "DEV", "username": "dev", "scope": "superuser"}
        )

    def force(self, token: Token) -> None:
        self.token = token

    def __call__(self, authorization: Optional[str]) -> Token:
        return self.token


class TokenVerifier(BaseTokenVerifier):
    """A class for verifying OAuth2 Access Tokens from AWS Cognito

    The verification steps followed are documented here:

    https://docs.aws.amazon.com/cognito/latest/developerguide/amazon- ⏎
    cognito-user-pools-using-tokens-verifying-a-jwt.html
    """

    # allow 2 minutes leeway for verifying token expiry:
    LEEWAY = 120

    def __init__(
        self, settings: TokenVerifierSettings, logger: Optional[logging.Logger] = None
    ):
        self.settings = settings
        self.jwk_client = PyJWKClient(f"{settings.issuer}/.well-known/jwks.json")

    def __call__(self, authorization: Optional[str]) -> Token:
        # Step 0: retrieve the token from the Authorization header
        # See https://tools.ietf.org/html/rfc6750#section-2.1,
        # Bearer is case-sensitive and there is exactly 1 separator after.
        if authorization is None:
            logger.info("Missing Authorization header")
            raise Unauthorized()
        jwt_str = authorization[7:] if authorization.startswith("Bearer") else None
        if jwt_str is None:
            logger.info("Authorization does not start with 'Bearer '")
            raise Unauthorized()

        # Step 1: Confirm the structure of the JWT. This check is part of get_kid since
        # jwt.get_unverified_header will raise a JWTError if the structure is wrong.
        try:
            key = self.get_key(jwt_str)  # JSON Web Key
        except PyJWTError as e:
            logger.info("Token is invalid: %s", e)
            raise Unauthorized()
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
            logger.info("Token is invalid: %s", e)
            raise Unauthorized()
        # Step 3: Verify additional claims. At this point, we have passed
        # verification, so unverified claims may be used safely.
        self.verify_token_use(claims)
        try:
            token = Token(claims=claims)
        except ValidationError as e:
            logger.info("Token is invalid: %s", e)
            raise Unauthorized()
        self.verify_scope(token.scope)
        # Step 4: Authorization: verify user id ('sub' claim) against 'admin_users'
        self.authorize_user(token.user)
        return token

    def get_key(self, token) -> jwt.PyJWK:
        """Return the JSON Web KEY (JWK) corresponding to kid."""
        return self.jwk_client.get_signing_key_from_jwt(token)

    def verify_token_use(self, claims: Dict[str, Any]) -> None:
        """Check the token_use claim."""
        if claims["token_use"] != "access":
            logger.info("Token has invalid token_use claim: %s", claims["token_use"])
            raise Unauthorized()

    def verify_scope(self, claims_scope: FrozenSet[str]) -> None:
        """Parse scopes and optionally check scope claim."""
        if self.settings.scope is None:
            return
        if self.settings.scope not in claims_scope:
            logger.info("Token is missing '%s' scope", self.settings.scope)
            raise Unauthorized()

    def authorize_user(self, user: User) -> None:
        if self.settings.admin_users is None:
            return
        if user.id not in self.settings.admin_users:
            raise PermissionDenied()
