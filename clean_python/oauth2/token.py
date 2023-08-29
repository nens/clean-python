from typing import Optional

from pydantic import validator

from clean_python import Json
from clean_python import Scope
from clean_python import Tenant
from clean_python import User
from clean_python import ValueObject

__all__ = ["Token"]


class Token(ValueObject):
    claims: Json

    @validator("claims")
    def validate_claims(cls, v):
        if not isinstance(v, dict):
            return v
        assert v.get("sub"), "missing 'sub' claim"
        assert v.get("scope"), "missing 'scope' claim"
        assert v.get("username"), "missing 'username' claim"
        if v.get("tenant"):
            assert v.get("tenant_name"), "missing 'tenant_name' claim"
        return v

    @property
    def user(self) -> User:
        return User(id=self.claims["sub"], name=self.claims["username"])

    @property
    def scope(self) -> Scope:
        return frozenset(self.claims["scope"].split(" "))

    @property
    def tenant(self) -> Optional[Tenant]:
        if self.claims.get("tenant"):
            return Tenant(id=self.claims["tenant"], name=self.claims["tenant_name"])
        else:
            return None
