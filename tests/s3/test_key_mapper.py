import pytest
from pydantic import ValidationError

from clean_python import ctx
from clean_python import Tenant
from clean_python.s3 import KeyMapper


@pytest.fixture
def tenant():
    ctx.tenant = Tenant(id=1, name="test")
    yield
    ctx.tenant = None


@pytest.mark.parametrize(
    "pattern,multitenant,ids,expected",
    [
        ("{}", False, ("foo",), "foo"),
        ("{}", False, (25,), "25"),
        ("bla/{}", False, ("foo",), "bla/foo"),
        ("{}", True, ("foo",), "tenant-1/foo"),
        ("raster-{}/{}", False, (25, "foo"), "raster-25/foo"),
        ("raster-{}/{}", True, (25, "foo"), "tenant-1/raster-25/foo"),
    ],
)
def test_to_key(pattern, multitenant, ids, expected, tenant):
    mapper = KeyMapper(pattern=pattern, multitenant=multitenant)
    assert mapper.to_key(*ids) == expected


@pytest.mark.parametrize(
    "pattern,multitenant,ids,expected",
    [
        ("{}", False, (), ""),
        ("bla/{}", False, (), "bla/"),
        ("{}", True, (), "tenant-1/"),
        ("raster-{}/{}", False, (25,), "raster-25/"),
        ("raster-{}/{}", True, (25,), "tenant-1/raster-25/"),
    ],
)
def test_to_key_prefix(pattern, multitenant, ids, expected, tenant):
    mapper = KeyMapper(pattern=pattern, multitenant=multitenant)
    assert mapper.to_key_prefix(*ids) == expected


@pytest.mark.parametrize(
    "pattern,multitenant,expected,key",
    [
        ("{}", False, ("foo",), "foo"),
        ("{}", False, (25,), "25"),
        ("bla/{}", False, ("foo",), "bla/foo"),
        ("{}", True, ("foo",), "tenant-1/foo"),
        ("raster-{}/{}", False, (25, "foo"), "raster-25/foo"),
        ("raster-{}/{}", True, (25, "foo"), "tenant-1/raster-25/foo"),
    ],
)
def test_from_key(pattern, multitenant, expected, key, tenant):
    mapper = KeyMapper(pattern=pattern, multitenant=multitenant)
    assert mapper.from_key(key) == expected


def test_from_key_wrong_tenant(tenant):
    mapper = KeyMapper(pattern="{}", multitenant=True)
    with pytest.raises(ValueError):
        mapper.from_key("tenant-2/foo")


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
@pytest.mark.parametrize("multitenant", [True, False])
def test_get_named_pattern(pattern, multitenant, names, expected):
    mapper = KeyMapper(pattern=pattern, multitenant=multitenant)
    assert mapper.get_named_pattern(*names) == expected
