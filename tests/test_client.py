from __future__ import annotations

from datetime import date

import httpx
import pytest

from psx_data_sync.client import PSXClient, PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.parser import classify_html
from psx_data_sync.state import ClientFailureKind, ContentClassification


BOOTSTRAP_PAGE = b'<script>window.__ps = {"_k":"test-request-id"};</script>'


def test_client_posts_correct_url_and_form_payload(fixture_bytes) -> None:
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            observed["bootstrap_url"] = str(request.url)
            return httpx.Response(
                200,
                content=BOOTSTRAP_PAGE,
                headers={"Set-Cookie": "psx-session=test-cookie; Path=/"},
            )
        observed["method"] = request.method
        observed["url"] = str(request.url)
        observed["body"] = request.content
        observed["user_agent"] = request.headers["user-agent"]
        observed["accept"] = request.headers["accept"]
        observed["requested_with"] = request.headers["x-requested-with"]
        observed["request_id"] = request.headers["x-req-id"]
        observed["cookie"] = request.headers["cookie"]
        return httpx.Response(200, content=fixture_bytes("valid_market.html"))

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    settings = Settings(user_agent="test-agent")
    client = PSXClient(settings, http_client=http_client)

    response = client.fetch(date(2026, 8, 5))

    assert response.status_code == 200
    assert classify_html(response.content) is ContentClassification.EQUITY_ROWS
    assert observed == {
        "bootstrap_url": settings.historical_url,
        "method": "POST",
        "url": settings.historical_url,
        "body": b"date=2026-08-05",
        "user_agent": "test-agent",
        "accept": "text/html,application/xhtml+xml",
        "requested_with": "XMLHttpRequest",
        "request_id": "test-request-id",
        "cookie": "psx-session=test-cookie",
    }
    http_client.close()


def test_empty_market_html_remains_content_not_http_failure(fixture_bytes) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        content = (
            BOOTSTRAP_PAGE
            if request.method == "GET"
            else fixture_bytes("empty_shell.html")
        )
        return httpx.Response(200, content=content)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = PSXClient(Settings(), http_client=http_client)

    response = client.fetch(date(2026, 8, 8))

    assert (
        classify_html(response.content)
        is ContentClassification.EMPTY_MARKET_RESPONSE
    )
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
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, content=BOOTSTRAP_PAGE)
        return httpx.Response(status, text="error")

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler)
    )
    client = PSXClient(Settings(), http_client=http_client)

    with pytest.raises(PSXClientError) as raised:
        client.fetch(date(2026, 8, 5))

    assert raised.value.http_status == status
    assert raised.value.retryable is False
    http_client.close()
