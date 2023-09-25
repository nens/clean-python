import pytest
from pydantic import ValidationError

from clean_python.s3 import KeyMapper


@pytest.mark.parametrize(
    "pattern,ids,expected",
    [
        ("{}", ("foo",), "foo"),
        ("{}", (25,), "25"),
        ("bla/{}", ("foo",), "bla/foo"),
        ("raster-{}/{}", (25, "foo"), "raster-25/foo"),
    ],
)
def test_to_key(pattern, ids, expected):
    mapper = KeyMapper(pattern=pattern)
    assert mapper.to_key(*ids) == expected


@pytest.mark.parametrize(
    "pattern,ids,expected",
    [
        ("{}", (), ""),
        ("bla/{}", (), "bla/"),
        ("raster-{}/{}", (25,), "raster-25/"),
    ],
)
def test_to_key_prefix(pattern, ids, expected):
    mapper = KeyMapper(pattern=pattern)
    assert mapper.to_key_prefix(*ids) == expected


@pytest.mark.parametrize(
    "pattern,expected,key",
    [
        ("{}", ("foo",), "foo"),
        ("{}", (25,), "25"),
        ("bla/{}", ("foo",), "bla/foo"),
        ("raster-{}/{}", (25, "foo"), "raster-25/foo"),
    ],
)
def test_from_key(pattern, expected, key):
    mapper = KeyMapper(pattern=pattern)
    assert mapper.from_key(key) == expected


@pytest.mark.parametrize("pattern", ["", "/{}", "{}-bla", "{a}/{}"])
def test_validate_pattern(pattern):
    with pytest.raises(ValidationError):
        KeyMapper(pattern=pattern)


@pytest.mark.parametrize(
    "pattern,names,expected",
    [
        ("{}", ("name",), "{name}"),
        ("raster-{}/{}", ("id", "name"), "raster-{id}/{name}"),
    ],
)
def test_get_named_pattern(pattern, names, expected):
    mapper = KeyMapper(pattern=pattern)
    assert mapper.get_named_pattern(*names) == expected
