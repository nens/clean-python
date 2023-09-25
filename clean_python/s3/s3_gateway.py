# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

import logging
from pathlib import Path
from typing import List
from typing import Optional

import inject
from botocore.exceptions import ClientError
from pydantic import AnyHttpUrl

from clean_python import ctx
from clean_python import DoesNotExist
from clean_python import Filter
from clean_python import Gateway
from clean_python import Id
from clean_python import Json
from clean_python import PageOptions

from .s3_provider import S3BucketProvider

DEFAULT_EXPIRY = 3600  # in seconds
DEFAULT_TIMEOUT = 1.0
AWS_LIMIT = 1000  # max s3 keys per request


__all__ = ["S3Gateway"]

logger = logging.getLogger(__name__)


class S3Gateway(Gateway):
    """The interface to S3 Buckets.

    The standard Gateway interface is only partially implemented:

    - get() and filter() return metadata
    - add(), update(), upsert() are not implemented
    - remove() works as expected

    For actually getting the object data either use the download_file()
    or upload_file() or create a presigned url and hand that over to
    the client.
    """

    def __init__(
        self,
        provider_override: Optional[S3BucketProvider] = None,
        multitenant: bool = False,
    ):
        self.provider_override = provider_override
        self.multitenant = multitenant

    @property
    def provider(self):
        return self.provider_override or inject.instance(S3BucketProvider)

    def _id_to_key(self, id: Id) -> str:
        if not self.multitenant:
            return str(id)
        if ctx.tenant is None:
            raise RuntimeError(f"{self.__class__} requires a tenant in the context")
        return f"tenant-{ctx.tenant.id}/{id}"

    def _key_to_id(self, key: str) -> Id:
        return key.split("/", 1)[1] if self.multitenant else key

    async def get(self, id: Id) -> Optional[Json]:
        async with self.provider.client as client:
            try:
                result = await client.head_object(
                    Bucket=self.provider.bucket, Key=self._id_to_key(id)
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return None
                else:
                    raise e
        return {
            "id": str(id),
            "last_modified": result["LastModified"],
            "etag": result["ETag"].strip('"'),
            "size": result["ContentLength"],
        }

    async def filter(
        self,
        filters: List[Filter],
        params: Optional[PageOptions] = PageOptions(limit=AWS_LIMIT),
    ) -> List[Json]:
        assert params is not None, "pagination is required for S3Gateway"
        assert params.limit <= AWS_LIMIT, f"max {AWS_LIMIT} keys for S3Gateway"
        assert params.offset == 0, "no 'offset' pagination for S3Gateway"
        assert params.order_by == "id", "can order by 'id' only for S3Gateway"
        kwargs = {
            "Bucket": self.provider.bucket,
            "MaxKeys": params.limit,
            "Prefix": self.filters_to_prefix(filters),
        }
        if params.cursor is not None:
            kwargs["StartAfter"] = self._id_to_key(params.cursor)
        async with self.provider.client as client:
            result = await client.list_objects_v2(**kwargs)
        # Example response:
        #     {
        #         'Key': 'object-in-s3',
        #         'LastModified': datetime.datetime(..., tzinfo=utc),
        #         'ETag': '"acbd18db4cc2f85cedef654fccc4a4d8"',
        #         'Size': 3, 'StorageClass':
        #         'STANDARD',
        #         'Owner': {...}
        #     }
        return [
            {
                "id": self._key_to_id(x["Key"]),
                "last_modified": x["LastModified"],
                "etag": x["ETag"].strip('"'),
                "size": x["Size"],
            }
            for x in result.get("Contents", [])
        ]

    async def remove(self, id: Id) -> bool:
        async with self.provider.client as client:
            await client.delete_object(
                Bucket=self.provider.bucket,
                Key=self._id_to_key(id),
            )
        # S3 doesn't tell us if the object was there in the first place
        return True

    async def remove_multiple(self, ids: List[Id]) -> None:
        if len(ids) == 0:
            return
        assert len(ids) <= AWS_LIMIT, f"max {AWS_LIMIT} keys for S3Gateway"
        async with self.provider.client as client:
            await client.delete_objects(
                Bucket=self.provider.bucket,
                Delete={
                    "Objects": [{"Key": self._id_to_key(x)} for x in ids],
                    "Quiet": True,
                },
            )

    async def _create_presigned_url(
        self,
        id: Id,
        client_method: str,
    ) -> AnyHttpUrl:
        async with self.provider.client as client:
            return await client.generate_presigned_url(
                client_method,
                Params={"Bucket": self.provider.bucket, "Key": self._id_to_key(id)},
                ExpiresIn=DEFAULT_EXPIRY,
            )

    async def create_download_url(self, id: Id) -> AnyHttpUrl:
        return await self._create_presigned_url(id, "get_object")

    async def create_upload_url(self, id: Id) -> AnyHttpUrl:
        return await self._create_presigned_url(id, "put_object")

    async def download_file(self, id: Id, file_path: Path) -> None:
        if file_path.exists():
            raise FileExistsError()
        try:
            async with self.provider.client as client:
                await client.download_file(
                    Bucket=self.provider.bucket,
                    Key=self._id_to_key(id),
                    Filename=str(file_path),
                )
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                file_path.unlink(missing_ok=True)
                raise DoesNotExist("object")
            else:
                raise e

    async def upload_file(self, id: Id, file_path: Path) -> None:
        if not file_path.is_file():
            raise FileNotFoundError()
        async with self.provider.client as client:
            await client.upload_file(
                Bucket=self.provider.bucket,
                Key=self._id_to_key(id),
                Filename=str(file_path),
            )

    def filters_to_prefix(self, filters: List[Filter]) -> str:
        if len(filters) == 0:
            return self._id_to_key("")
        elif len(filters) > 1:
            raise NotImplementedError("More than 1 filter is not supported")
        (filter,) = filters
        if filter.field == "prefix":
            assert len(filter.values) == 1
            return self._id_to_key(filter.values[0])
        else:
            raise NotImplementedError(f"Unsupported filter '{filter.field}'")

    async def remove_filtered(self, filters: List[Filter]) -> None:
        kwargs = {
            "Bucket": self.provider.bucket,
            "MaxKeys": AWS_LIMIT,
            "Prefix": self.filters_to_prefix(filters),
        }
        async with self.provider.client as client:
            while True:
                result = await client.list_objects_v2(**kwargs)
                contents = result.get("Contents", [])
                if contents:
                    await client.delete_objects(
                        Bucket=self.provider.bucket,
                        Delete={
                            "Objects": [{"Key": x["Key"]} for x in contents],
                            "Quiet": True,
                        },
                    )
                if len(contents) < AWS_LIMIT:
                    break
                kwargs["StartAfter"] = contents[-1]["Key"]
