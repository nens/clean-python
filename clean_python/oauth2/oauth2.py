# (c) Nelen & Schuurmans

from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional

import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from pydantic import AnyHttpUrl
from pydantic import BaseModel

from clean_python import PermissionDenied
from clean_python import Unauthorized

from .claims import Claims
from .claims import Tenant

__all__ = ["TokenVerifier", "TokenVerifierSettings", "OAuth2SPAClientSettings"]


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


class TokenVerifier:
    """A class for verifying OAuth2 Access Tokens from AWS Cognito

    The verification steps followed are documented here:

    https://docs.aws.amazon.com/cognito/latest/developerguide/amazon- ⏎
    cognito-user-pools-using-tokens-verifying-a-jwt.html
    """

    # allow 2 minutes leeway for verifying token expiry:
    LEEWAY = 120

    def __init__(self, settings: TokenVerifierSettings):
        self.settings = settings
        self.jwk_client = PyJWKClient(f"{settings.issuer}/.well-known/jwks.json")

    def __call__(self, authorization: Optional[str]) -> Claims:
        # Step 0: retrieve the token from the Authorization header
        # See https://tools.ietf.org/html/rfc6750#section-2.1,
        # Bearer is case-sensitive and there is exactly 1 separator after.
        if authorization is None:
            raise Unauthorized()
        token = authorization[7:] if authorization.startswith("Bearer") else None
        if token is None:
            raise Unauthorized()

        # Step 1: Confirm the structure of the JWT. This check is part of get_kid since
        # jwt.get_unverified_header will raise a JWTError if the structure is wrong.
        try:
            key = self.get_key(token)  # JSON Web Key
        except PyJWTError:
            # logger.info("Token is invalid: %s", e)
            raise Unauthorized()
        # Step 2: Validate the JWT signature and standard claims
        try:
            claims = jwt.decode(
                token,
                key.key,
                algorithms=self.settings.algorithms,
                issuer=self.settings.issuer,
                leeway=self.LEEWAY,
                options={
                    "require": ["exp", "iss", "sub", "scope", "token_use"],
                },
            )
        except PyJWTError:
            # logger.info("Token is invalid: %s", e)
            raise Unauthorized()
        # Step 3: Verify additional claims. At this point, we have passed
        # verification, so unverified claims may be used safely.
        scope = self.parse_scope(claims)
        tenant = self.parse_tenant(claims)
        self.verify_token_use(claims)
        self.verify_scope(scope)
        # Step 4: Authorization: verify 'sub' claim against 'admin_users'
        self.verify_sub(claims)
        return Claims(scope=scope, tenant=tenant)

    def get_key(self, token) -> jwt.PyJWK:
        """Return the JSON Web KEY (JWK) corresponding to kid."""
        return self.jwk_client.get_signing_key_from_jwt(token)

    def verify_token_use(self, claims: Dict) -> None:
        """Check the token_use claim."""
        if claims["token_use"] != "access":
            # logger.info("Token has invalid token_use claim: %s", claims["token_use"])
            raise Unauthorized()

    def verify_scope(self, claims_scope: FrozenSet[str]) -> None:
        """Parse scopes and optionally check scope claim."""
        if self.settings.scope is None:
            return
        if self.settings.scope not in claims_scope:
            # logger.info("Token has invalid scope claim: %s", claims["scope"])
            raise Unauthorized()

    def verify_sub(self, claims: Dict) -> None:
        """The subject (sub) claim should be in a hard-coded whitelist."""
        if self.settings.admin_users is None:
            return
        if claims.get("sub") not in self.settings.admin_users:
            # logger.info("User with sub %s is not authorized", claims.get("sub"))
            raise PermissionDenied()

    def parse_scope(self, claims: Dict) -> FrozenSet[str]:
        return frozenset(claims["scope"].split(" "))

    def parse_tenant(self, claims: Dict) -> Optional[Tenant]:
        if claims.get("tenant"):
            tenant = Tenant(id=claims["tenant"], name=claims.get("tenant_name", ""))
        else:
            tenant = None
        return tenant
