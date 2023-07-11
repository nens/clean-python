# (c) Nelen & Schuurmans

import asyncio

from dramatiq.middleware import SkipMessage

from .async_actor import async_actor


@async_actor(
    retry_when=lambda x, y: isinstance(y, KeyError),
    max_retries=1,
)
async def sleep_task(seconds: int, return_value=None, event="success"):
    event = event.lower()
    if event == "success":
        await asyncio.sleep(int(seconds))
    elif event == "crash":
        import ctypes

        ctypes.string_at(0)  # segfault
    elif event == "skip":
        raise SkipMessage("skipping")
    elif event == "retry":
        raise KeyError("will-retry")
    else:
        raise ValueError(f"Unknown event '{event}'")

    return return_value
