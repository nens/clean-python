# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import io

import boto3
import pytest
from botocore.exceptions import ClientError

from clean_python import DoesNotExist
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


def create_s3_test_bucket(url, access_key, secret_key, bucket, **kwargs):
    s3 = boto3.resource(
        "s3",
        endpoint_url=url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    bucket_obj = s3.Bucket(bucket)

    ensure_exists(bucket_obj)
    ensure_empty(bucket_obj)
    return bucket_obj


def cleanup_s3_test_bucket(bucket):
    ensure_not_exists(bucket)


def ensure_exists(bucket):
    try:
        bucket.create()
    except ClientError as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            pass


def ensure_not_exists(bucket):
    ensure_empty(bucket)
    try:
        bucket.delete()
    except ClientError as e:
        if "NoSuchBucket" in str(e):
            pass


def ensure_empty(bucket):
    try:
        bucket.objects.all().delete()
    except ClientError as e:
        if "NoSuchBucket" in str(e):
            pass
        raise e


@pytest.fixture(scope="session")
def s3_bucket_session_scoped(s3_settings):
    bucket = create_s3_test_bucket(**s3_settings)
    yield bucket
    cleanup_s3_test_bucket(bucket)


@pytest.fixture
def s3_bucket(s3_bucket_session_scoped, s3_settings):
    return boto3.resource(
        "s3",
        endpoint_url=s3_settings["url"],
        aws_access_key_id=s3_settings["access_key"],
        aws_secret_access_key=s3_settings["secret_key"],
    ).Bucket(s3_settings["bucket"])


@pytest.fixture
def s3_provider(s3_settings):
    return S3BucketProvider(S3BucketOptions(**s3_settings))


@pytest.fixture
def s3_gateway(s3_provider):
    return S3Gateway(s3_provider)


@pytest.fixture
def object_in_s3(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "object-in-s3")
    return "object-in-s3"


@pytest.fixture
def test_s3_path():
    return "path1/"


@pytest.fixture
def object_in_s3_path(s3_bucket, test_s3_path):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), test_s3_path + "object-in-s3")
    return test_s3_path + "object-in-s3"


@pytest.fixture
def upload_file(tmp_path):
    path = tmp_path / "test-upload.txt"
    path.write_bytes(b"foo")
    return path


async def test_upload_file(s3_gateway: S3Gateway, s3_bucket, upload_file):
    object_name = "test-upload-file"

    await s3_gateway.upload_file(object_name, upload_file)

    assert s3_bucket.Object(object_name).content_length == 3


async def test_upload_file_does_not_exist(s3_gateway: S3Gateway, s3_bucket, tmp_path):
    path = tmp_path / "test-upload.txt"
    object_name = "test-upload-file"

    with pytest.raises(FileNotFoundError):
        await s3_gateway.upload_file(object_name, path)


async def test_download_file(s3_gateway: S3Gateway, object_in_s3, tmp_path):
    path = tmp_path / "test-download.txt"

    await s3_gateway.download_file(object_in_s3, path)

    assert path.read_bytes() == b"foo"


async def test_download_file_path_already_exists(
    s3_gateway: S3Gateway, object_in_s3, tmp_path
):
    path = tmp_path / "test-download.txt"
    path.write_bytes(b"bar")

    with pytest.raises(FileExistsError):
        await s3_gateway.download_file(object_in_s3, path)

    assert path.read_bytes() == b"bar"


async def test_download_file_does_not_exist(s3_gateway: S3Gateway, s3_bucket, tmp_path):
    path = tmp_path / "test-download-does-not-exist.txt"

    with pytest.raises(DoesNotExist):
        await s3_gateway.download_file("some-nonexisting", path)

    assert not path.exists()


async def test_remove(s3_gateway: S3Gateway, s3_bucket, object_in_s3):
    await s3_gateway.remove(object_in_s3)

    with pytest.raises(ClientError):
        assert s3_bucket.Object(object_in_s3).content_length


async def test_remove_does_not_exist(s3_gateway: S3Gateway, s3_bucket):
    await s3_gateway.remove("non-existing")


async def test_delete_path(tmp_path, s3_gateway: S3Gateway, s3_bucket):
    object_paths = ["path1/object-in-s3", "path11/object-in-s3"]
    for path in object_paths:
        s3_bucket.upload_fileobj(io.BytesIO(b"foo"), path)

    # Delete path1
    await s3_gateway.delete_path("path1/")

    # The file prefixed with path1/ should not exist anymore
    download_file = tmp_path / "test-download.txt"
    with pytest.raises(DoesNotExist):
        await s3_gateway.download_file(object_paths[0], download_file)

    # The other file should still exist
    await s3_gateway.download_file(object_paths[1], download_file)
    assert download_file.read_bytes() == b"foo"


@pytest.mark.parametrize("prefix", ["path1", "/"])
async def test_delete_path_with_incorrect_path_prefix(s3_gateway: S3Gateway, prefix):
    with pytest.raises(ValueError):
        await s3_gateway.delete_path(prefix)


@pytest.mark.parametrize(
    "n_files,page_size",
    [
        (0, 10),  # no files
        (3, 2),  # last page is truncated
        (6, 3),  # last page has no content
    ],
)
async def test_delete_path_pagination(
    s3_gateway: S3Gateway, s3_bucket, upload_file, test_s3_path, n_files, page_size
):
    for n in range(0, n_files):
        await s3_gateway.upload_file(test_s3_path + str(n), upload_file)

    # Confirm object count
    res = s3_bucket.objects.filter(Prefix=test_s3_path)
    assert len(list(res)) == n_files

    # Delete them
    await s3_gateway.delete_path(test_s3_path, page_size=page_size)

    # All object were deleted
    res = s3_bucket.objects.filter(Prefix=test_s3_path)
    assert len(list(res)) == 0


async def test_get_size_2(s3_gateway: S3Gateway, s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"a"), "raster-1/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"ab"), "raster-2/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"abc"), "raster-2/foo")
    s3_bucket.upload_fileobj(io.BytesIO(b"abcde"), "raster-2/bz")

    actual = await s3_gateway.get_size(prefix="raster-2/")

    assert actual == 10
