# (c) Nelen & Schuurmans

import logging
from typing import TYPE_CHECKING

import boto3
from botocore.client import Config

from clean_python import SyncProvider

from .s3_bucket_options import S3BucketOptions

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

__all__ = ["S3BucketOptions", "SyncS3BucketProvider"]

logger = logging.getLogger(__name__)


class SyncS3BucketProvider(SyncProvider):
    def __init__(self, options: S3BucketOptions):
        self.options = options
        self._client = None

    @property
    def bucket(self) -> str:
        return self.options.bucket

    @property
    def client(self) -> "S3Client":
        assert (
            self._client is not None
        ), "S3BucketProvider not connected, call connect() first"
        # "Clients are generally thread-safe"
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
        return self._client

    def connect(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=self.options.url,
            aws_access_key_id=self.options.access_key,
            aws_secret_access_key=self.options.secret_key,
            region_name=self.options.region,
            config=Config(
                s3={"addressing_style": "virtual"},  # "path" will become deprecated
                signature_version="s3v4",  # for minio
                retries={
                    "max_attempts": 4,  # 1 try and up to 3 retries
                    "mode": "adaptive",
                },
            ),
            use_ssl=self.options.url.startswith("https"),
        )

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
