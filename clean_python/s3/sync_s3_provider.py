# (c) Nelen & Schuurmans

import logging
from typing import TYPE_CHECKING

import boto3
from botocore.client import Config

from .s3_bucket_options import S3BucketOptions

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

__all__ = ["S3BucketOptions", "SyncS3BucketProvider"]

logger = logging.getLogger(__name__)


class SyncS3BucketProvider:
    def __init__(self, options: S3BucketOptions):
        self.options = options

    @property
    def bucket(self) -> str:
        return self.options.bucket

    @property
    def client(self) -> "S3Client":
        return boto3.client(
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
