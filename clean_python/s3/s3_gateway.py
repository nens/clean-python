# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import logging
from pathlib import Path

import inject
from botocore.exceptions import ClientError
from pydantic import AnyHttpUrl

from clean_python import DoesNotExist
from clean_python import Gateway

from .s3_provider import S3BucketProvider

DEFAULT_EXPIRY = 3600  # in seconds
DEFAULT_TIMEOUT = 1.0
AWS_LIMIT = 1000  # max s3 keys per request


__all__ = ["S3Gateway"]

logger = logging.getLogger(__name__)


class S3Gateway(Gateway):
    def __init__(self, provider_override: S3BucketProvider | None = None):
        self.provider_override = provider_override

    @property
    def provider(self):
        return self.provider_override or inject.instance(S3BucketProvider)

    async def _create_presigned_url(
        self, client_method: str, object_name: str
    ) -> AnyHttpUrl:
        async with self.provider.get_client() as client:
            return await client.generate_presigned_url(
                client_method,
                Params={"Bucket": self.provider.bucket, "Key": object_name},
                ExpiresIn=DEFAULT_EXPIRY,
            )

    async def create_download_url(self, id: str | int) -> AnyHttpUrl:
        return await self._create_presigned_url("get_object", str(id))

    async def create_upload_url(self, id: str | int) -> AnyHttpUrl:
        return await self._create_presigned_url("put_object", str(id))

    async def download_file(self, id: str | int, file_path: Path) -> None:
        if file_path.exists():
            raise FileExistsError()
        try:
            async with self.provider.get_client() as client:
                await client.download_file(
                    Bucket=self.provider.bucket,
                    Key=str(id),
                    Filename=str(file_path),
                )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                file_path.unlink(missing_ok=True)
                raise DoesNotExist("object")
            else:
                raise e

    async def upload_file(self, id: str | int, file_path: Path) -> None:
        if not file_path.is_file():
            raise FileNotFoundError()
        async with self.provider.get_client() as client:
            await client.upload_file(
                Bucket=self.provider.bucket,
                Key=str(id),
                Filename=str(file_path),
            )

    async def remove(self, id: str | int) -> None:
        async with self.provider.get_client() as client:
            await client.delete_object(
                Bucket=self.options.bucket,
                Key=str(id),
            )

    # https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder
    # https://stackoverflow.com/questions/54314563/how-to-get-more-than-1000-objects-from-s3-by-using-list-objects-v2
    async def delete_path(self, path_prefix: str, page_size: int = AWS_LIMIT) -> None:
        # path_prefix needs to end with "/" to prevent incorrect
        # deletes (raster-1 vs raster-11)
        if len(path_prefix) <= 1 or path_prefix[-1] != "/":
            raise ValueError(f"Incorrect path prefix: {path_prefix}")
        assert page_size <= AWS_LIMIT

        async with self.provider.get_client() as client:
            paginator = client.get_paginator("list_objects_v2")

            # Explicitly setting MaxKeys seems to require using
            # continuation tokens.
            async for page in paginator.paginate(
                Bucket=self.provider.bucket,
                Prefix=path_prefix,
                PaginationConfig={"PageSize": page_size},
            ):
                items = page.get("Contents", [])
                if len(items) == 0:
                    continue
                assert len(items) <= AWS_LIMIT  # needed for delete_objects to work
                await client.delete_objects(
                    Bucket=self.provider.bucket,
                    Delete={
                        "Objects": [{"Key": item["Key"]} for item in items],
                        "Quiet": True,
                    },
                )

    async def get_size(self, prefix: str = "") -> int:
        size = 0
        async with self.provider.get_client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=self.provider.bucket, Prefix=prefix
            ):
                for item in page.get("Contents", []):
                    size += item["Size"]
        return size
