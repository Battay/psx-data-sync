"""Reusable HTTP client for the PSX historical endpoint."""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from .config import MAX_RANGE_WORKERS, Settings
from .state import ClientFailureKind, FetchResponse


logger = logging.getLogger(__name__)


class PSXClientError(RuntimeError):
    """A classified request failure with an explicit retry policy."""

    def __init__(
        self,
        message: str,
        *,
        kind: ClientFailureKind,
        retryable: bool,
        http_status: int | None = None,
        response_bytes: int = 0,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable
        self.http_status = http_status
        self.response_bytes = response_bytes
        self.retry_after_seconds = retry_after_seconds

    @property
    def rate_limited(self) -> bool:
        return self.http_status == 429


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    try:
        seconds = float(stripped)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(stripped)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _classify_response(response: httpx.Response) -> FetchResponse:
    status = response.status_code
    response_bytes = len(response.content)
    if status == 429 or 500 <= status <= 599:
        raise PSXClientError(
            f"PSX returned retryable HTTP {status}",
            kind=ClientFailureKind.HTTP,
            retryable=True,
            http_status=status,
            response_bytes=response_bytes,
            retry_after_seconds=(
                _parse_retry_after(response.headers.get("Retry-After"))
                if status == 429
                else None
            ),
        )
    if status != 200:
        raise PSXClientError(
            f"PSX returned HTTP {status}",
            kind=ClientFailureKind.HTTP,
            retryable=False,
            http_status=status,
            response_bytes=response_bytes,
        )
    if not response.content.strip():
        raise PSXClientError(
            "PSX returned an empty response body",
            kind=ClientFailureKind.EMPTY_BODY,
            retryable=True,
            http_status=status,
            response_bytes=0,
        )
    return FetchResponse(status_code=status, content=response.content)


class PSXClient:
    """Connection-pooled, injectable client for one or more PSX requests."""

    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(
            timeout=httpx.Timeout(
                settings.request_timeout_seconds,
                connect=settings.connect_timeout_seconds,
            ),
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": settings.user_agent,
            },
        )

    def fetch(self, requested_date: date) -> FetchResponse:
        """POST one ISO date and classify HTTP/transport failures."""

        iso_date = requested_date.isoformat()
        try:
            response = self._client.post(
                self.settings.historical_url,
                data={"date": iso_date},
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": self.settings.user_agent,
                },
            )
        except httpx.TimeoutException as exc:
            raise PSXClientError(
                f"PSX request timed out: {exc}",
                kind=ClientFailureKind.TIMEOUT,
                retryable=True,
            ) from exc
        except httpx.RequestError as exc:
            raise PSXClientError(
                f"PSX connection failed: {exc}",
                kind=ClientFailureKind.CONNECTION,
                retryable=True,
            ) from exc

        status = response.status_code
        logger.info(
            "PSX response date=%s http_status=%s response_bytes=%s",
            iso_date,
            status,
            len(response.content),
        )
        return _classify_response(response)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "PSXClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncPSXClient:
    """One pooled asynchronous HTTP session shared by a bounded range run."""

    def __init__(
        self,
        settings: Settings,
        *,
        workers: int,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if workers < 1 or workers > MAX_RANGE_WORKERS:
            raise ValueError(
                f"workers must be between 1 and {MAX_RANGE_WORKERS}"
            )
        self.settings = settings
        self.workers = workers
        self._client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.request_timeout_seconds,
                connect=settings.connect_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=workers,
                max_keepalive_connections=workers,
            ),
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": settings.user_agent,
            },
        )
        self._closed = False

    async def fetch(self, requested_date: date) -> FetchResponse:
        iso_date = requested_date.isoformat()
        try:
            response = await self._client.post(
                self.settings.historical_url,
                data={"date": iso_date},
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": self.settings.user_agent,
                },
            )
        except httpx.TimeoutException as exc:
            raise PSXClientError(
                f"PSX request timed out: {exc}",
                kind=ClientFailureKind.TIMEOUT,
                retryable=True,
            ) from exc
        except httpx.RequestError as exc:
            raise PSXClientError(
                f"PSX connection failed: {exc}",
                kind=ClientFailureKind.CONNECTION,
                retryable=True,
            ) from exc

        logger.info(
            "PSX async response date=%s http_status=%s response_bytes=%s",
            iso_date,
            response.status_code,
            len(response.content),
        )
        return _classify_response(response)

    async def close(self) -> None:
        if not self._closed:
            await self._client.aclose()
            self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> "AsyncPSXClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
