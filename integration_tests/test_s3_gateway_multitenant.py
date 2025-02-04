# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import io
from datetime import datetime

import pytest

from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import PageOptions
from clean_python import Tenant
from clean_python.s3 import S3BucketOptions
from clean_python.s3 import S3BucketProvider
from clean_python.s3 import S3Gateway


@pytest.fixture
def tenant_context():
    ctx.tenant = Tenant(id=22, name="foo")
    yield ctx.tenant
    ctx.tenant = None


@pytest.fixture
async def s3_provider(s3_bucket, s3_settings, tenant_context):
    # wipe contents before each test
    s3_bucket.objects.all().delete()
    provider = S3BucketProvider(S3BucketOptions(**s3_settings))
    await provider.connect()
    yield provider
    await provider.disconnect()


@pytest.fixture
def s3_gateway(s3_provider):
    return S3Gateway(s3_provider, multitenant=True)


async def test_upload_file_uses_tenant(s3_gateway: S3Gateway, local_file, s3_bucket):
    object_name = "test-upload-file"

    await s3_gateway.upload_file(object_name, local_file)

    assert s3_bucket.Object("tenant-22/test-upload-file").content_length == 3


async def test_download_file_uses_tenant(
    s3_gateway: S3Gateway, object_in_s3_tenant, tmp_path
):
    path = tmp_path / "test-download.txt"

    await s3_gateway.download_file(object_in_s3_tenant, path)

    assert path.read_bytes() == b"foo"


async def test_download_file_different_tenant(
    s3_gateway: S3Gateway, s3_bucket, tmp_path, object_in_s3_other_tenant
):
    path = tmp_path / "test-download.txt"

    with pytest.raises(DoesNotExist):
        await s3_gateway.download_file("object-in-s3", path)

    assert not path.exists()


async def test_remove_uses_tenant(
    s3_gateway: S3Gateway, s3_bucket, object_in_s3_tenant
):
    await s3_gateway.remove(object_in_s3_tenant)

    assert await s3_gateway.get(object_in_s3_tenant) is None


async def test_remove_other_tenant(
    s3_gateway: S3Gateway, s3_bucket, object_in_s3_other_tenant
):
    await s3_gateway.remove(object_in_s3_other_tenant)

    # it is still there
    assert s3_bucket.Object("tenant-222/object-in-s3").content_length == 3


@pytest.fixture
def multiple_objects(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"a"), "tenant-22/raster-1/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"ab"), "tenant-222/raster-2/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"abc"), "tenant-22/raster-2/foo")
    s3_bucket.upload_fileobj(io.BytesIO(b"abcde"), "tenant-22/raster-2/bz")
    return ["raster-1/bla", "raster-2/bla", "raster-2/foo", "raster-2/bz"]


async def test_remove_multiple_multitenant(
    s3_gateway: S3Gateway, multiple_objects, s3_bucket
):
    await s3_gateway.remove_multiple(multiple_objects[:2])

    assert await s3_gateway.get(multiple_objects[0]) is None

    # the other-tenant object is still there
    assert s3_bucket.Object("tenant-222/raster-2/bla").content_length == 2


async def test_filter_multitenant(s3_gateway: S3Gateway, multiple_objects):
    actual = await s3_gateway.filter([], params=PageOptions(limit=10))
    assert len(actual) == 3
    assert actual[0]["id"] == "raster-1/bla"


async def test_filter_with_prefix_multitenant(s3_gateway: S3Gateway, multiple_objects):
    actual = await s3_gateway.filter(
        [Filter(field="prefix", values=["raster-2/"])], params=PageOptions(limit=10)
    )
    assert len(actual) == 2
    assert actual[0]["id"] == "raster-2/bz"
    assert actual[1]["id"] == "raster-2/foo"


async def test_filter_with_cursor_multitenant(s3_gateway: S3Gateway, multiple_objects):
    actual = await s3_gateway.filter(
        [], params=PageOptions(limit=3, cursor="raster-2/bz")
    )
    assert len(actual) == 1
    assert actual[0]["id"] == "raster-2/foo"


async def test_get_multitenant(s3_gateway: S3Gateway, object_in_s3_tenant):
    actual = await s3_gateway.get(object_in_s3_tenant)
    assert actual["id"] == object_in_s3_tenant
    assert isinstance(actual["last_modified"], datetime)
    assert actual["etag"] == "acbd18db4cc2f85cedef654fccc4a4d8"
    assert actual["size"] == 3


async def test_get_other_tenant(s3_gateway: S3Gateway, object_in_s3_other_tenant):
    actual = await s3_gateway.get(object_in_s3_other_tenant)
    assert actual is None


async def test_remove_filtered_all(s3_gateway: S3Gateway, multiple_objects):
    await s3_gateway.remove_filtered([])

    # tenant 22 is completely wiped
    for i in (0, 2, 3):
        assert await s3_gateway.get(multiple_objects[i]) is None

    # object of tenant 222 is still there
    ctx.tenant = Tenant(id=222, name="other")
    await s3_gateway.get("raster-2/bla") is not None


async def test_remove_filtered_prefix(s3_gateway: S3Gateway, multiple_objects):
    await s3_gateway.remove_filtered([Filter(field="prefix", values=["raster-2/"])])

    assert await s3_gateway.get("raster-1/bla") is not None
    assert await s3_gateway.get("raster-2/foo") is None
    assert await s3_gateway.get("raster-2/bz") is None

    # object of tenant 222 is still there
    ctx.tenant = Tenant(id=222, name="other")
    await s3_gateway.get("raster-2/bla") is not None
