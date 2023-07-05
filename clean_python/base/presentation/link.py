# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

from pydantic import AnyHttpUrl
from typing_extensions import TypedDict


class Link(TypedDict):
    href: AnyHttpUrl
