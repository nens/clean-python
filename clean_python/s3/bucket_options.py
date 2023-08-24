# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

from typing import Optional

from clean_python import ValueObject

__all__ = ["BucketOptions"]


class BucketOptions(ValueObject):
    url: str
    access_key: str
    secret_key: str
    bucket: str
    region: Optional[str] = None
