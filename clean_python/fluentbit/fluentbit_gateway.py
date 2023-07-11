# (c) Nelen & Schuurmans

from asgiref.sync import sync_to_async
from fluent.sender import FluentSender

from clean_python import Gateway
from clean_python import Json

__all__ = ["FluentbitGateway"]


class FluentbitGateway(Gateway):
    def __init__(self, tag: str, host: str, port: int):
        self._sender = FluentSender(tag, host=host, port=port)

    @sync_to_async
    def add(self, item: Json) -> Json:
        self._sender.emit(item.pop("tag_suffix", ""), item)
        return item
