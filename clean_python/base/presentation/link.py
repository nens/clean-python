# (c) Nelen & Schuurmans


from pydantic import AnyHttpUrl

from ..domain import ValueObject

__all__ = ["Link"]


class Link(ValueObject):
    href: AnyHttpUrl
