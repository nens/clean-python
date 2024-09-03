# (c) Nelen & Schuurmans

import logging
from typing import TYPE_CHECKING

try:
    import aioboto3
except ImportError:
    aioboto3 = None
from botocore.client import Config

from clean_python import Provider

from .s3_bucket_options import S3BucketOptions

if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

__all__ = ["S3BucketOptions", "S3BucketProvider"]

logger = logging.getLogger(__name__)


class S3BucketProvider(Provider):
    def __init__(self, options: S3BucketOptions):
        self.options = options

    async def connect(self) -> None:
        assert aioboto3 is not None
        self._session = aioboto3.Session()
        self._client = await self._session.client(
            "s3",
            endpoint_url=self.options.url,
            aws_access_key_id=self.options.access_key,
            aws_secret_access_key=self.options.secret_key,
            region_name=self.options.region,
            config=Config(
                s3={"addressing_style": self.options.addressing_style},
                signature_version="s3v4",  # for minio
                retries={
                    "max_attempts": 4,  # 1 try and up to 3 retries
                    "mode": "adaptive",
                },
            ),
            use_ssl=self.options.url.startswith("https"),
        ).__aenter__()

    async def disconnect(self) -> None:
        await self._client.close()

    @property
    def bucket(self) -> str:
        return self.options.bucket

    @property
    def client(self) -> "S3Client":
        return self._client
