# This module is a copy paste of test_s3_gateway.py

import io
from datetime import datetime
from typing import Iterator
from unittest import mock

import pytest

from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import PageOptions
from clean_python.s3 import S3BucketOptions
from clean_python.s3 import SyncS3BucketProvider
from clean_python.s3 import SyncS3Gateway


@pytest.fixture
def s3_provider(s3_bucket, s3_settings) -> Iterator[SyncS3BucketProvider]:
    # wipe contents before each test
    s3_bucket.objects.all().delete()
    provider = SyncS3BucketProvider(S3BucketOptions(**s3_settings))
    provider.connect()
    yield provider
    provider.disconnect()


@pytest.fixture
def s3_gateway(s3_provider) -> SyncS3Gateway:
    return SyncS3Gateway(s3_provider)


@pytest.fixture
def object_in_s3(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"foo"), "object-in-s3")
    return "object-in-s3"


@pytest.fixture
def local_file(tmp_path):
    path = tmp_path / "test-upload.txt"
    path.write_bytes(b"foo")
    return path


def test_upload_file(s3_gateway: SyncS3Gateway, local_file):
    object_name = "test-upload-file"

    s3_gateway.upload_file(object_name, local_file)

    assert (s3_gateway.get(object_name))["size"] == 3


def test_upload_file_does_not_exist(s3_gateway: SyncS3Gateway, tmp_path):
    path = tmp_path / "test-upload.txt"
    object_name = "test-upload-file"

    with pytest.raises(FileNotFoundError):
        s3_gateway.upload_file(object_name, path)


def test_download_file(s3_gateway: SyncS3Gateway, object_in_s3, tmp_path):
    path = tmp_path / "test-download.txt"

    s3_gateway.download_file(object_in_s3, path)

    assert path.read_bytes() == b"foo"


def test_download_file_path_already_exists(
    s3_gateway: SyncS3Gateway, object_in_s3, tmp_path
):
    path = tmp_path / "test-download.txt"
    path.write_bytes(b"bar")

    with pytest.raises(FileExistsError):
        s3_gateway.download_file(object_in_s3, path)

    assert path.read_bytes() == b"bar"


def test_download_file_does_not_exist(s3_gateway: SyncS3Gateway, s3_bucket, tmp_path):
    path = tmp_path / "test-download-does-not-exist.txt"

    with pytest.raises(DoesNotExist):
        s3_gateway.download_file("some-nonexisting", path)

    assert not path.exists()


def test_remove(s3_gateway: SyncS3Gateway, s3_bucket, object_in_s3):
    s3_gateway.remove(object_in_s3)

    assert s3_gateway.get(object_in_s3) is None


def test_remove_does_not_exist(s3_gateway: SyncS3Gateway, s3_bucket):
    s3_gateway.remove("non-existing")


@pytest.fixture
def multiple_objects(s3_bucket):
    s3_bucket.upload_fileobj(io.BytesIO(b"a"), "raster-1/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"ab"), "raster-2/bla")
    s3_bucket.upload_fileobj(io.BytesIO(b"abc"), "raster-2/foo")
    s3_bucket.upload_fileobj(io.BytesIO(b"abcde"), "raster-2/bz")
    return ["raster-1/bla", "raster-2/bla", "raster-2/foo", "raster-2/bz"]


def test_remove_multiple(s3_gateway: SyncS3Gateway, multiple_objects):
    s3_gateway.remove_multiple(multiple_objects[:2])

    for key in multiple_objects[:2]:
        assert s3_gateway.get(key) is None

    for key in multiple_objects[2:]:
        assert s3_gateway.get(key) is not None


def test_remove_multiple_empty_list(s3_gateway: SyncS3Gateway, s3_bucket):
    s3_gateway.remove_multiple([])


def test_create_download_url(s3_gateway: SyncS3Gateway, object_in_s3):
    actual = s3_gateway.create_download_url(object_in_s3)

    assert object_in_s3 in actual
    assert "response-content-disposition=attachment%3B%20filename%3D" not in actual
    assert "X-Amz-Expires=3600" in actual
    assert "X-Amz-SignedHeaders=host" in actual


def test_create_download_url_with_filename(s3_gateway: SyncS3Gateway, object_in_s3):
    actual = s3_gateway.create_download_url(object_in_s3, "file.txt")

    assert "file.txt" in actual
    assert "response-content-disposition=attachment%3B%20filename%3Dfile.txt" in actual
    assert object_in_s3 in actual
    assert "X-Amz-Expires=3600" in actual
    assert "X-Amz-SignedHeaders=host" in actual


def test_create_upload_url(s3_gateway: SyncS3Gateway, object_in_s3):
    actual = s3_gateway.create_upload_url(object_in_s3)

    assert object_in_s3 in actual
    assert "X-Amz-Expires=3600" in actual
    assert "X-Amz-SignedHeaders=host" in actual


def test_remove_filtered_all(s3_gateway: SyncS3Gateway, multiple_objects):
    s3_gateway.remove_filtered([])

    for key in multiple_objects:
        assert s3_gateway.get(key) is None


def test_remove_filtered_prefix(s3_gateway: SyncS3Gateway, multiple_objects):
    s3_gateway.remove_filtered([Filter(field="prefix", values=["raster-2/"])])

    assert s3_gateway.get(multiple_objects[0]) is not None
    for key in multiple_objects[1:]:
        assert s3_gateway.get(key) is None


@mock.patch("clean_python.s3.s3_gateway.AWS_LIMIT", new=1)
def test_remove_filtered_pagination(s3_gateway: SyncS3Gateway, multiple_objects):
    s3_gateway.remove_filtered([Filter(field="prefix", values=["raster-2/"])])

    assert s3_gateway.get(multiple_objects[0]) is not None
    for key in multiple_objects[1:]:
        assert s3_gateway.get(key) is None


def test_filter(s3_gateway: SyncS3Gateway, multiple_objects):
    actual = s3_gateway.filter([], params=PageOptions(limit=10))
    assert len(actual) == 4
    assert actual[0]["id"] == "raster-1/bla"
    assert isinstance(actual[0]["last_modified"], datetime)
    assert actual[0]["etag"] == "0cc175b9c0f1b6a831c399e269772661"
    assert actual[0]["size"] == 1


def test_filter_empty(s3_gateway: SyncS3Gateway, s3_bucket):
    actual = s3_gateway.filter([], params=PageOptions(limit=10))
    assert actual == []


def test_filter_with_limit(s3_gateway: SyncS3Gateway, multiple_objects):
    actual = s3_gateway.filter([], params=PageOptions(limit=2))
    assert len(actual) == 2
    assert actual[0]["id"] == "raster-1/bla"
    assert actual[1]["id"] == "raster-2/bla"


def test_filter_with_cursor(s3_gateway: SyncS3Gateway, multiple_objects):
    actual = s3_gateway.filter([], params=PageOptions(limit=3, cursor="raster-2/bla"))
    assert len(actual) == 2
    assert actual[0]["id"] == "raster-2/bz"
    assert actual[1]["id"] == "raster-2/foo"


def test_filter_by_prefix(s3_gateway: SyncS3Gateway, multiple_objects):
    actual = s3_gateway.filter([Filter(field="prefix", values=["raster-1/"])])
    assert len(actual) == 1

    actual = s3_gateway.filter([Filter(field="prefix", values=["raster-2/"])])
    assert len(actual) == 3


def test_get(s3_gateway: SyncS3Gateway, object_in_s3):
    actual = s3_gateway.get(object_in_s3)
    assert actual["id"] == "object-in-s3"
    assert isinstance(actual["last_modified"], datetime)
    assert actual["etag"] == "acbd18db4cc2f85cedef654fccc4a4d8"
    assert actual["size"] == 3


def test_get_does_not_exist(s3_gateway: SyncS3Gateway):
    actual = s3_gateway.get("non-existing")
    assert actual is None
