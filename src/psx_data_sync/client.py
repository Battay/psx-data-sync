"""Reusable HTTP client for the PSX historical endpoint."""

from __future__ import annotations

import logging
from datetime import date

import httpx

from .config import Settings
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
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable
        self.http_status = http_status
        self.response_bytes = response_bytes


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
        if status == 429 or 500 <= status <= 599:
            raise PSXClientError(
                f"PSX returned retryable HTTP {status}",
                kind=ClientFailureKind.HTTP,
                retryable=True,
                http_status=status,
                response_bytes=len(response.content),
            )
        if status != 200:
            raise PSXClientError(
                f"PSX returned HTTP {status}",
                kind=ClientFailureKind.HTTP,
                retryable=False,
                http_status=status,
                response_bytes=len(response.content),
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

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "PSXClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
