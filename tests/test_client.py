from __future__ import annotations

from datetime import date

import httpx
import pytest

from psx_data_sync.client import PSXClient, PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.state import ClientFailureKind


def test_client_posts_correct_url_and_form_payload(fixture_bytes) -> None:
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["method"] = request.method
        observed["url"] = str(request.url)
        observed["body"] = request.content
        observed["user_agent"] = request.headers["user-agent"]
        return httpx.Response(200, content=fixture_bytes("valid_market.html"))

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    settings = Settings(user_agent="test-agent")
    client = PSXClient(settings, http_client=http_client)

    response = client.fetch(date(2026, 8, 5))

    assert response.status_code == 200
    assert observed == {
        "method": "POST",
        "url": settings.historical_url,
        "body": b"date=2026-08-05",
        "user_agent": "test-agent",
    }
    http_client.close()


def test_timeout_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("too slow", request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.kind is ClientFailureKind.TIMEOUT
    assert raised.value.retryable is True
    http_client.close()


def test_connection_failure_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.kind is ClientFailureKind.CONNECTION
    assert raised.value.retryable is True
    http_client.close()


def test_empty_body_is_retryable() -> None:
    http_client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b""))
    )
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.kind is ClientFailureKind.EMPTY_BODY
    assert raised.value.retryable is True
    http_client.close()


@pytest.mark.parametrize("status", [429, 500, 502, 599])
def test_retryable_http_statuses(status: int) -> None:
    http_client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, text="error"))
    )
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.http_status == status
    assert raised.value.retryable is True
    http_client.close()


@pytest.mark.parametrize("status", [400, 401, 404])
def test_non_retryable_http_statuses(status: int) -> None:
    http_client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, text="error"))
    )
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.http_status == status
    assert raised.value.retryable is False
    http_client.close()
