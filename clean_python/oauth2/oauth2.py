# (c) Nelen & Schuurmans

from typing import Dict
from typing import List

import jwt
from jwt import PyJWKClient
from jwt.exceptions import PyJWTError
from pydantic import AnyHttpUrl
from pydantic import BaseModel

from clean_python.base.domain.exceptions import PermissionDenied
from clean_python.base.domain.exceptions import Unauthorized

__all__ = ["OAuth2Settings", "OAuth2AccessTokenVerifier"]


class OAuth2Settings(BaseModel):
    client_id: str
    issuer: str
    resource_server_id: str
    token_url: AnyHttpUrl
    authorization_url: AnyHttpUrl
    algorithms: List[str] = ["RS256"]
    admin_users: List[str]


class OAuth2AccessTokenVerifier:
    """A class for verifying OAuth2 Access Tokens from AWS Cognito

    The verification steps followed are documented here:

    https://docs.aws.amazon.com/cognito/latest/developerguide/amazon- ⏎
    cognito-user-pools-using-tokens-verifying-a-jwt.html
    """

    # allow 2 minutes leeway for verifying token expiry:
    LEEWAY = 120

    def __init__(
        self,
        scope: str,
        issuer: str,
        resource_server_id: str,
        algorithms: List[str],
        admin_users: List[str],
    ):
        self.scope = scope
        self.issuer = issuer
        self.algorithms = algorithms
        self.resource_server_id = resource_server_id
        self.admin_users = admin_users
        self.jwk_client = PyJWKClient(f"{issuer}/.well-known/jwks.json")

    def __call__(self, token: str) -> Dict:
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
                algorithms=self.algorithms,
                issuer=self.issuer,
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
        self.verify_token_use(claims)
        self.verify_scope(claims)
        # Step 4: Authorization: we currently work with a hardcoded
        # list of users ('sub' claims)
        self.authorize(claims)
        return claims

    def get_key(self, token) -> jwt.PyJWK:
        """Return the JSON Web KEY (JWK) corresponding to kid."""
        return self.jwk_client.get_signing_key_from_jwt(token)

    def verify_token_use(self, claims):
        """Check the token_use claim."""
        if claims["token_use"] != "access":
            # logger.info("Token has invalid token_use claim: %s", claims["token_use"])
            raise Unauthorized()

    def verify_scope(self, claims):
        """Check scope claim.

        Cognito includes the resource server id inside the scope, like this:

           raster.lizard.net/*.readwrite
        """
        if f"{self.resource_server_id}{self.scope}" not in claims["scope"].split(" "):
            # logger.info("Token has invalid scope claim: %s", claims["scope"])
            raise Unauthorized()

    def authorize(self, claims):
        """The subject (sub) claim should be in a hard-coded whitelist."""
        if claims.get("sub") not in self.admin_users:
            # logger.info("User with sub %s is not authorized", claims.get("sub"))
            raise PermissionDenied()
