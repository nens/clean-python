from unittest.mock import Mock

import pytest

from clean_python.s3 import SyncS3BucketProvider
from clean_python.s3 import SyncS3Gateway
from clean_python.s3.sync_s3_gateway import DEFAULT_EXPIRY


@pytest.fixture
def provider() -> SyncS3BucketProvider:
    return Mock(SyncS3BucketProvider, bucket="S3Bucket")


@pytest.fixture
def gateway(provider: Mock) -> SyncS3Gateway:
    return SyncS3Gateway(provider)


def test_create_download_url(gateway: SyncS3Gateway, provider: Mock):
    provider.client.generate_presigned_url.return_value = (
        "https://s3.amazonaws.com/object_in_s3"
    )

    actual = gateway.create_download_url("object-in-s3")

    assert actual == provider.client.generate_presigned_url.return_value
    provider.client.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={"Bucket": "S3Bucket", "Key": "object-in-s3"},
        ExpiresIn=DEFAULT_EXPIRY,
    )


def test_create_download_url_with_filename(gateway: SyncS3Gateway, provider: Mock):
    provider.client.generate_presigned_url.return_value = "https://s3.com/S3Object"

    actual = gateway.create_download_url("S3Object", "file.txt")

    assert actual == provider.client.generate_presigned_url.return_value
    provider.client.generate_presigned_url.assert_called_once_with(
        "get_object",
        Params={
            "Bucket": "S3Bucket",
            "Key": "S3Object",
            "ResponseContentDisposition": "attachment; filename=file.txt",
        },
        ExpiresIn=DEFAULT_EXPIRY,
    )


def test_create_upload_url(gateway: SyncS3Gateway, provider: Mock):
    provider.client.generate_presigned_url.return_value = "https://s3.com/S3Object"

    actual = gateway.create_upload_url("S3Object")

    assert actual == provider.client.generate_presigned_url.return_value
    provider.client.generate_presigned_url.assert_called_once_with(
        "put_object",
        Params={"Bucket": "S3Bucket", "Key": "S3Object"},
        ExpiresIn=DEFAULT_EXPIRY,
    )
