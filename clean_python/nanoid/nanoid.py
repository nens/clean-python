# (c) Nelen & Schuurmans

from typing import Annotated

from nanoid import generate as _generate
from pydantic import StringConstraints

__all__ = ["NanoId", "random_nanoid"]


NanoId = Annotated[str, StringConstraints(pattern="^[A-Za-z0-9_-]+$")]

# Ref. https://digitalbazaar.github.io/base58-spec/
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def random_nanoid(size: int, alphabet: str = BASE58) -> NanoId:
    return _generate(alphabet, size)
