# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import io
from datetime import datetime

import boto3
import pytest
from botocore.exceptions import ClientError

from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import PageOptions
from clean_python import Tenant
from clean_python.s3 import S3BucketOptions
from clean_python.s3 import S3BucketProvider
from clean_python.s3 import S3Gateway


@pytest.fixture(scope="session")
def s3_settings(s3_url):
    minio_settings = {
        "url": s3_url,
        "access_key": "cleanpython",
        "secret_key": "cleanpython",
        "bucket": "cleanpython-test",
        "region": None,
    }
    if not minio_settings["bucket"].endswith("-test"):  # type: ignore
        pytest.exit("Not running against a test minio bucket?! 😱")
    return minio_settings.copy()


@pytest.fixture(scope="session")
def s3_bucket(s3_settings):
    s3 = boto3.resource(
        "s3",
        endpoint_url=s3_settings["url"],
        aws_access_key_id=s3_settings["access_key"],
        aws_secret_access_key=s3_settings["secret_key"],
    )
    bucket = s3.Bucket(s3_settings["bucket"])

    # ensure existence
    try:
        bucket.create()
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            pass
    return bucket


@pytest.fixture
def s3_provider(s3_bucket, s3_settings):
    # wipe contents before each test
    s3_bucket.objects.all().delete()
    # set up a tenant
    ctx.tenant = Tenant(id=22, name="foo")
    yield S3BucketProvider(S3BucketOptions(**s3_settings))
    ctx.tenant = None


@pytest.fixture
def s3_gateway(s3_provider):
    return S3Gateway(s3_provider, multitenant=True)


@pytest.fixture
def object_in_s3(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "tenant-22/object-in-s3")
    return "object-in-s3"


@pytest.fixture
def object_in_s3_other_tenant(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "tenant-222/object-in-s3")
    return "object-in-s3"


@pytest.fixture
def local_file(tmp_path):
    path = tmp_path / "test-upload.txt"
    path.write_bytes(b"foo")
    return path


async def test_upload_file_uses_tenant(s3_gateway: S3Gateway, local_file, s3_bucket):
    object_name = "test-upload-file"

    await s3_gateway.upload_file(object_name, local_file)

    assert s3_bucket.Object("tenant-22/test-upload-file").content_length == 3


async def test_download_file_uses_tenant(s3_gateway: S3Gateway, object_in_s3, tmp_path):
    path = tmp_path / "test-download.txt"

    await s3_gateway.download_file(object_in_s3, path)

    assert path.read_bytes() == b"foo"


async def test_download_file_different_tenant(
    s3_gateway: S3Gateway, s3_bucket, tmp_path, object_in_s3_other_tenant
):
    path = tmp_path / "test-download.txt"

    with pytest.raises(DoesNotExist):
        await s3_gateway.download_file("object-in-s3", path)

    assert not path.exists()


async def test_remove_uses_tenant(s3_gateway: S3Gateway, s3_bucket, object_in_s3):
    await s3_gateway.remove(object_in_s3)

    assert await s3_gateway.get(object_in_s3) is None


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


async def test_get_multitenant(s3_gateway: S3Gateway, object_in_s3):
    actual = await s3_gateway.get(object_in_s3)
    assert actual["id"] == object_in_s3
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
