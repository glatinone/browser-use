from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union


class AsyncEventStreamHook:
    """Wrap an async iterator and emit lifecycle events to a handler.

    Events: on_start(), on_item(item), on_error(exc), on_complete()
    The handler may be sync or async. Failures in the handler are swallowed.
    """

    def __init__(self, handler: Optional[Callable[..., Union[None, Awaitable[None]]]] = None) -> None:
        self._handler = handler

    async def _emit(self, event: str, **payload: Any) -> None:
        if not self._handler:
            return
        try:
            res = self._handler(event=event, **payload)
            if asyncio.iscoroutine(res):
                await res  # type: ignore[func-returns-value]
        except Exception:
            # Never break the stream due to handler errors
            return

    async def run(self, source: AsyncIterator[Any]) -> AsyncIterator[Any]:
        await self._emit("on_start")
        try:
            async for item in source:
                await self._emit("on_item", item=item)
                yield item
        except Exception as exc:  # noqa: BLE001
            await self._emit("on_error", error=exc)
            raise
        else:
            await self._emit("on_complete")
