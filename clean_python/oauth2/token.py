from typing import Optional

from pydantic import field_validator

from clean_python import Json
from clean_python import Scope
from clean_python import Tenant
from clean_python import User
from clean_python import ValueObject

__all__ = ["Token"]


class Token(ValueObject):
    claims: Json

    @field_validator("claims")
    @classmethod
    def validate_claims(cls, v):
        if not isinstance(v, dict):
            return v
        assert v.get("sub"), "missing 'sub' claim"
        assert v.get("scope"), "missing 'scope' claim"
        assert v.get("username") or v.get(
            "client_id"
        ), "missing 'username' / 'client_id' claim"
        if v.get("tenant"):
            assert v.get("tenant_name"), "missing 'tenant_name' claim"
        return v

    @property
    def user(self) -> User:
        return User(
            id=self.claims["sub"],
            name=self.claims.get("username") or self.claims["client_id"],
        )

    @property
    def scope(self) -> Scope:
        return frozenset(self.claims["scope"].split(" "))

    @property
    def tenant(self) -> Optional[Tenant]:
        if self.claims.get("tenant"):
            return Tenant(id=self.claims["tenant"], name=self.claims["tenant_name"])
        else:
            return None
