# (c) Nelen & Schuurmans

from typing import TypedDict

from pydantic import AnyHttpUrl

__all__ = ["Link"]


class Link(TypedDict):
    href: AnyHttpUrl
