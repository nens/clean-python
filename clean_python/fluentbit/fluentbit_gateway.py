# (c) Nelen & Schuurmans

import time
from typing import Tuple

from asgiref.sync import sync_to_async
from fluent.sender import FluentSender

from clean_python import Gateway
from clean_python import Json
from clean_python import SyncGateway

__all__ = ["FluentbitGateway", "SyncFluentbitGateway"]


def unpack_item(item: Json) -> Tuple[str, float, Json]:
    data = item.copy()
    label = data.pop("tag_suffix", "")
    timestamp = data.pop("time", None)
    if timestamp is None:
        timestamp = time.time()
    return label, timestamp, data


class SyncFluentbitGateway(SyncGateway):
    def __init__(self, tag: str, host: str, port: int):
        self._sender = FluentSender(
            tag, host=host, port=port, nanosecond_precision=True
        )

    def add(self, item: Json):
        label, timestamp, data = unpack_item(item)
        self._sender.emit_with_time(label, timestamp, data)
        return {**data, "time": timestamp, "tag_suffix": label}


class FluentbitGateway(Gateway):
    def __init__(self, tag: str, host: str, port: int):
        self._sync_gateway = SyncFluentbitGateway(tag, host, port)

    @sync_to_async
    def _add(self, item: Json) -> Json:
        return self._sync_gateway.add(item)

    async def add(self, item: Json) -> Json:
        return await self._add(item)
