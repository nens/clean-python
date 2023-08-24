# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import logging
from pathlib import Path

import aioboto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pydantic import AnyHttpUrl

from clean_python import DoesNotExist

from .bucket_options import BucketOptions

DEFAULT_EXPIRY = 3600  # in seconds
DEFAULT_TIMEOUT = 1.0
AWS_LIMIT = 1000  # max s3 keys per request


__all__ = ["S3Client"]

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self, options: BucketOptions):
        self.options = options

    async def __aenter__(self) -> "S3Client":
        session = aioboto3.Session()
        self._client = await session.client(
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
        ).__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._client.__aexit__(*args, **kwargs)
        del self._client

    async def create_upload_url(self, object_name: str) -> AnyHttpUrl:
        return await self._client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self.options.bucket, "Key": object_name},
            ExpiresIn=DEFAULT_EXPIRY,
        )

    async def download_file(self, object_name: str, file_path: Path) -> None:
        if file_path.exists():
            raise FileExistsError()
        try:
            await self._client.download_file(
                self.options.bucket,
                object_name,
                str(file_path),
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                file_path.unlink(missing_ok=True)
                raise DoesNotExist("object")
            else:
                raise e

    async def upload_file(self, file_path: Path, object_name: str) -> None:
        if not file_path.is_file():
            raise FileNotFoundError()
        await self._client.upload_file(
            str(file_path),
            self.options.bucket,
            object_name,
        )

    async def delete_file(self, object_name: str) -> None:
        await self._client.delete_object(
            Bucket=self.options.bucket,
            Key=object_name,
        )

    # https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder
    # https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2
    async def delete_path(self, path_prefix: str, page_size: int = AWS_LIMIT) -> None:
        # path_prefix needs to end with "/" to prevent incorrect
        # deletes (raster-1 vs raster-11)
        if len(path_prefix) <= 1 or path_prefix[-1] != "/":
            raise ValueError(f"Incorrect path prefix: {path_prefix}")
        assert page_size <= AWS_LIMIT

        paginator = self._client.get_paginator("list_objects_v2")

        # Explicitly setting MaxKeys seems to require using
        # continuation tokens.
        async for page in paginator.paginate(
            Bucket=self.options.bucket,
            Prefix=path_prefix,
            PaginationConfig={"PageSize": page_size},
        ):
            items = page.get("Contents", [])
            if len(items) == 0:
                continue
            assert len(items) <= AWS_LIMIT  # needed for delete_objects to work
            await self._client.delete_objects(
                Bucket=self.options.bucket,
                Delete={
                    "Objects": [{"Key": item["Key"]} for item in items],
                    "Quiet": True,
                },
            )

    async def get_size(self, prefix: str = "") -> int:
        size = 0
        paginator = self._client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self.options.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                size += item["Size"]
        return size
