import asyncio
import pytest

from browser_use.middleware.async_event_hook import AsyncEventStreamHook


@pytest.mark.asyncio
async def test_emits_events_and_yields_items():
    events: list[tuple[str, dict]] = []

    def handler(**kw):
        events.append((kw["event"], {k: v for k, v in kw.items() if k != "event"}))

    async def producer():
        for i in range(3):
            await asyncio.sleep(0)
            yield i

    hook = AsyncEventStreamHook(handler)
    out = []
    async for x in hook.run(producer()):
        out.append(x)

    assert out == [0, 1, 2]
    names = [e[0] for e in events]
    assert names[0] == "on_start"
    assert names[-1] == "on_complete"
    assert [n for n in names if n == "on_item"] == ["on_item", "on_item", "on_item"]


@pytest.mark.asyncio
async def test_propagates_errors_and_emits_on_error():
    seen: dict[str, int] = {"error": 0}

    async def handler(**kw):
        if kw["event"] == "on_error":
            seen["error"] += 1

    async def bad():
        raise RuntimeError("boom")
        yield  # pragma: no cover

    hook = AsyncEventStreamHook(handler)
    with pytest.raises(RuntimeError):
        async for _ in hook.run(bad()):
            pass
    assert seen["error"] == 1
