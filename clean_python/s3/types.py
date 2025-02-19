# (c) Nelen & Schuurmans

from typing import TypedDict

__all__ = ["CompletedPart"]


class CompletedPart(TypedDict):
    etag: str
    part_number: int
