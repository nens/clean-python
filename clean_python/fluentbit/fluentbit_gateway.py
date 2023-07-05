# -*- coding: utf-8 -*-
# (c) Nelen & Schuurmans

from typing import Any, Dict

from asgiref.sync import sync_to_async
from fluent.sender import FluentSender

from .gateway import Gateway

Json = Dict[str, Any]


class FluentbitGateway(Gateway):
    def __init__(self, tag: str, host: str, port: int):
        self._sender = FluentSender(tag, host=host, port=port)

    @sync_to_async
    def add(self, item: Json) -> Json:
        self._sender.emit(item.pop("tag_suffix", ""), item)
        return item
