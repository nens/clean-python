# (c) Nelen & Schuurmans

from typing import Annotated

from nanoid import generate as _generate
from pydantic import StringConstraints

__all__ = ["NanoId", "random_nanoid"]


NanoId = Annotated[str, StringConstraints(pattern="^[A-Za-z0-9_-]+$")]

# Ref. https://digitalbazaar.github.io/base58-spec/
BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def random_nanoid(size: int, alphabet: str = BASE58) -> NanoId:
    """Generate a random string (NanoID) based on the base58 alphabet.

    Recommended sizes to have <1% collision probability:

    - 6 characters if the expected number of records is below 27K
    - 8 characters if the expected number of records is below 1M
    - 10 characters if the expected number of records is below 93M
    - 12 characters if the expected number of records is below 5B

    Ref. https://zelark.github.io/nano-id-cc/
    """
    return _generate(alphabet, size)
