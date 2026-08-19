from __future__ import annotations

from datetime import date

import httpx
import pytest

from psx_data_sync.client import AsyncPSXClient, PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.state import ClientFailureKind


@pytest.mark.asyncio
async def test_async_client_reuses_one_session_for_multiple_dates(
    fixture_bytes,
) -> None:
    bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(200, content=fixture_bytes("valid_market.html"))

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncPSXClient(Settings(), workers=2, http_client=http_client)

    async with client:
        await client.fetch(date(2026, 8, 4))
        await client.fetch(date(2026, 8, 5))

    assert bodies == [b"date=2026-08-04", b"date=2026-08-05"]
    assert client.is_closed
    assert http_client.is_closed


@pytest.mark.asyncio
async def test_async_client_records_numeric_retry_after() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="slow down", headers={"Retry-After": "2.5"})

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncPSXClient(Settings(), workers=1, http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        await client.fetch(date(2026, 8, 5))

    assert raised.value.http_status == 429
    assert raised.value.rate_limited
    assert raised.value.retry_after_seconds == 2.5
    await client.close()


@pytest.mark.asyncio
async def test_async_timeout_is_retryable() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncPSXClient(Settings(), workers=1, http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        await client.fetch(date(2026, 8, 5))

    assert raised.value.kind is ClientFailureKind.TIMEOUT
    assert raised.value.retryable
    await client.close()
