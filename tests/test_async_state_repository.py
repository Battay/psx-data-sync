from __future__ import annotations

import asyncio
import threading

import pytest

from psx_data_sync.state_db import AsyncStateRepository


@pytest.mark.asyncio
async def test_serialized_thread_write_drains_before_cancellation_escapes() -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    repository = AsyncStateRepository(object())  # type: ignore[arg-type]

    def blocked_write() -> str:
        started.set()
        assert release.wait(timeout=2)
        finished.set()
        return "committed"

    task = asyncio.create_task(repository.run_serialized(blocked_write))
    assert await asyncio.to_thread(started.wait, 2)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert not finished.is_set()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()

