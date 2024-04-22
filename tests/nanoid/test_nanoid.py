import random
import re

import pytest
from pydantic import TypeAdapter
from pydantic import ValidationError

from clean_python.nanoid import NanoId
from clean_python.nanoid import random_nanoid

NanoIdTA = TypeAdapter(NanoId)


@pytest.mark.parametrize("value", ["a", "V1StGXR8_Z5jdHi6B-myT"])
def test_valid_nanoid(value: str):
    NanoIdTA.validate_python(value)


@pytest.mark.parametrize("value", ["", "V1StGXR8_Z5jdHi6B/myT"])
def test_invalid_nanoid(value: str):
    with pytest.raises(ValidationError):
        NanoIdTA.validate_python(value)


@pytest.mark.parametrize("size", [1, 8, 21])
def test_random_nanoid_size(size: int):
    assert len(random_nanoid(size)) == size


def test_random_nanoid_collision():
    assert random_nanoid(8) != random_nanoid(8)


def test_random_nanoid_alphabet():
    assert re.match("[a-g3-5]+", random_nanoid(21, alphabet="abcdefg345"))


def test_random_not_software():
    random.seed(1234)
    x = random_nanoid(21)
    random.seed(1234)
    y = random_nanoid(21)
    assert x != y
