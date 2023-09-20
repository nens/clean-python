import re
from typing import Tuple

from pydantic import field_validator

from clean_python import ctx
from clean_python import DomainService
from clean_python import Id

__all__ = ["KeyMapper"]


def _maybe_coerce_int(x: str) -> Id:
    try:
        return int(x)
    except ValueError:
        return x


class KeyMapper(DomainService):
    """Maps one or multiple ids to a string and vice versa.

    The mapping is configured using a python formatting string with standard
    {} placeholders. Additionally, the key can be prefixed with a tenant id
    when multitenant=True.
    """

    pattern: str = "{}"
    multitenant: bool = False

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, v):
        if isinstance(v, str):
            assert not v.startswith("/"), "pattern should not start with '/'"
            assert v.endswith("{}"), "pattern cannot have a suffix"
            try:
                v.format(*((2,) * v.count("{}")))
            except KeyError:
                raise ValueError("invalid pattern")
        return v

    @property
    def n_placeholders(self) -> int:
        return self.pattern.count("{}")

    def get_pattern(self) -> str:
        return "tenant-{}/" + self.pattern if self.multitenant else self.pattern

    def get_named_pattern(self, *names: str) -> str:
        return self.pattern.format(*[f"{{{x}}}" for x in names])

    def get_regex(self) -> str:
        return "^" + self.get_pattern().replace("{}", "(.+)") + "$"

    def get_tenant_id(self) -> int:
        if ctx.tenant is None:
            raise RuntimeError(f"{self} requires a tenant in the context")
        return ctx.tenant.id

    def to_key(self, *args: Id) -> str:
        assert len(args) == self.n_placeholders
        if self.multitenant:
            args = (self.get_tenant_id(),) + args
        pattern = self.get_pattern()
        return pattern.format(*args)

    def to_key_prefix(self, *args: Id) -> str:
        return self.to_key(*(args + ("",)))

    def from_key(self, key: str) -> Tuple[Id, ...]:
        match = re.fullmatch(self.get_regex(), key)
        if match is None:
            raise ValueError("key does not match expected pattern")
        ids = match.groups()
        if self.multitenant:
            if int(ids[0]) != self.get_tenant_id():
                raise ValueError("key had unexpected tenant id")
            ids = ids[1:]
        return tuple(_maybe_coerce_int(x) for x in ids)
