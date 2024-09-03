from typing import Literal

from clean_python import ValueObject

__all__ = ["S3BucketOptions"]


class S3BucketOptions(ValueObject):
    url: str
    access_key: str
    secret_key: str
    bucket: str
    region: str | None = None
    addressing_style: Literal[
        "virtual", "path"
    ] = "virtual"  # path is deprecated for AWS S3
