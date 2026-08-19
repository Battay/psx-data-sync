"""Canonical per-date orchestration for synchronous D1 and asynchronous D2."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from .client import AsyncPSXClient, PSXClient, PSXClientError
from .config import Settings
from .exporter import inspect_existing_canonical_file, save_canonical_csv
from .parser import classify_html, parse_equity_rows
from .state import (
    ContentClassification,
    DownloadResult,
    DownloadStatus,
    FetchResponse,
    SaveStatus,
)
from .validator import validate_rows


logger = logging.getLogger(__name__)
ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_requested_date(value: str, *, today: date | None = None) -> date:
    """Require a real, non-future calendar date in strict ISO form."""

    if not ISO_DATE_PATTERN.fullmatch(value):
        raise ValueError("date must use YYYY-MM-DD format")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid calendar date: {value}") from exc
    current_date = date.today() if today is None else today
    if parsed > current_date:
        raise ValueError(f"future dates are not allowed: {value}")
    return parsed


@dataclass(slots=True)
class _DownloadContext:
    requested_date: str
    started: float = field(default_factory=time.perf_counter)
    network_ms: float = 0.0
    parse_ms: float = 0.0
    validation_ms: float = 0.0
    save_ms: float = 0.0
    attempts: int = 0
    http_status: int | None = None
    response_bytes: int = 0
    cumulative_response_bytes: int = 0
    rate_limit_count: int = 0
    warnings: list[str] = field(default_factory=list)


class _BaseSingleDateDownloader:
    """Shared classification, validation, persistence, and result construction."""

    def __init__(
        self,
        settings: Settings,
        *,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self._random_value = random_value

    def _retry_delay(
        self,
        failed_attempt: int,
        retry_after_seconds: float | None = None,
    ) -> float:
        base = min(
            self.settings.retry_backoff_max_seconds,
            self.settings.retry_backoff_initial_seconds * (2 ** (failed_attempt - 1)),
        )
        floor = max(base, retry_after_seconds or 0.0)
        jitter_basis = base if base > 0 else floor
        return floor + (
            jitter_basis
            * self.settings.retry_jitter_fraction
            * self._random_value()
        )

    def _finish(
        self,
        context: _DownloadContext,
        status: DownloadStatus,
        *,
        parsed_row_count: int = 0,
        valid_row_count: int = 0,
        rejected_row_count: int = 0,
        saved_path: Path | None = None,
        checksum: str | None = None,
        locally_skipped: bool = False,
        error: str | None = None,
    ) -> DownloadResult:
        completed = DownloadResult(
            requested_date=context.requested_date,
            status=status,
            http_status=context.http_status,
            attempts=context.attempts,
            response_bytes=context.response_bytes,
            cumulative_response_bytes=context.cumulative_response_bytes,
            parsed_row_count=parsed_row_count,
            valid_row_count=valid_row_count,
            rejected_row_count=rejected_row_count,
            elapsed_ms=(time.perf_counter() - context.started) * 1000,
            network_ms=context.network_ms,
            parse_ms=context.parse_ms,
            validation_ms=context.validation_ms,
            save_ms=context.save_ms,
            saved_path=saved_path,
            checksum=checksum,
            rate_limit_count=context.rate_limit_count,
            locally_skipped=locally_skipped,
            warnings=tuple(context.warnings),
            error=error,
        )
        logger.info(
            "PSX date complete date=%s status=%s attempts=%s rate_limits=%s "
            "response_bytes=%s parsed_rows=%s valid_rows=%s rejected_rows=%s "
            "duration_ms=%.2f",
            context.requested_date,
            completed.status.value,
            completed.attempts,
            completed.rate_limit_count,
            completed.response_bytes,
            completed.parsed_row_count,
            completed.valid_row_count,
            completed.rejected_row_count,
            completed.elapsed_ms,
        )
        return completed

    def _record_client_error(
        self, context: _DownloadContext, error: PSXClientError
    ) -> None:
        context.http_status = error.http_status
        context.response_bytes = error.response_bytes
        context.cumulative_response_bytes += error.response_bytes
        if error.rate_limited:
            context.rate_limit_count += 1
        logger.warning(
            "PSX request failed date=%s attempt=%s retryable=%s "
            "retry_after=%s error=%s",
            context.requested_date,
            context.attempts,
            error.retryable,
            error.retry_after_seconds,
            error,
        )

    def _classify_response(
        self, context: _DownloadContext, response: FetchResponse
    ) -> ContentClassification:
        context.http_status = response.status_code
        context.response_bytes = len(response.content)
        context.cumulative_response_bytes += len(response.content)
        classification = classify_html(response.content)
        logger.info(
            "PSX content date=%s attempt=%s classification=%s response_bytes=%s",
            context.requested_date,
            context.attempts,
            classification.value,
            context.response_bytes,
        )
        return classification

    def _content_failure(
        self,
        context: _DownloadContext,
        classification: ContentClassification,
    ) -> DownloadResult:
        if classification is ContentClassification.EMPTY_MARKET_RESPONSE:
            context.warnings.append(
                "PSX returned a valid empty table on every completed content attempt; "
                "this is not proof of a non-trading day"
            )
            return self._finish(context, DownloadStatus.NON_TRADING_OR_EMPTY)
        description = classification.value.lower().replace("_", " ")
        return self._finish(
            context,
            DownloadStatus.PARSE_FAILURE,
            error=f"unexpected response content: {description}",
        )

    def _process_equity_content(
        self,
        context: _DownloadContext,
        parsed_date: date,
        response_content: bytes,
    ) -> DownloadResult:
        """The one canonical parse/validate/save path used by D1 and D2."""

        parse_started = time.perf_counter()
        try:
            parsed_rows = parse_equity_rows(response_content)
        except Exception as exc:
            context.parse_ms += (time.perf_counter() - parse_started) * 1000
            logger.exception("PSX parsing failed date=%s", context.requested_date)
            return self._finish(
                context, DownloadStatus.PARSE_FAILURE, error=f"parse failed: {exc}"
            )
        context.parse_ms += (time.perf_counter() - parse_started) * 1000

        validation_started = time.perf_counter()
        validation = validate_rows(parsed_rows)
        context.validation_ms += (
            time.perf_counter() - validation_started
        ) * 1000
        parsed_count = len(parsed_rows)
        valid_count = len(validation.valid_rows)
        rejected_count = len(validation.rejected_rows)
        logger.info(
            "PSX rows date=%s parsed_rows=%s valid_rows=%s rejected_rows=%s",
            context.requested_date,
            parsed_count,
            valid_count,
            rejected_count,
        )
        if rejected_count:
            context.warnings.append(f"{rejected_count} malformed row(s) rejected")
            for rejected in validation.rejected_rows[:5]:
                label = rejected.symbol or f"row {rejected.row_index}"
                context.warnings.append(
                    f"{label}: {'; '.join(rejected.reasons)}"
                )

        if valid_count == 0:
            return self._finish(
                context,
                DownloadStatus.VALIDATION_FAILURE,
                parsed_row_count=parsed_count,
                rejected_row_count=rejected_count,
                error="no valid equity rows remained after validation",
            )

        save_started = time.perf_counter()
        try:
            saved = save_canonical_csv(
                validation.valid_rows,
                parsed_date,
                self.settings.raw_output_dir,
                self.settings.canonical_columns,
            )
        except OSError as exc:
            context.save_ms += (time.perf_counter() - save_started) * 1000
            logger.exception("PSX CSV save failed date=%s", context.requested_date)
            return self._finish(
                context,
                DownloadStatus.SAVE_FAILURE,
                parsed_row_count=parsed_count,
                valid_row_count=valid_count,
                rejected_row_count=rejected_count,
                error=f"failed to save canonical CSV: {exc}",
            )
        context.save_ms += (time.perf_counter() - save_started) * 1000

        common = {
            "parsed_row_count": parsed_count,
            "valid_row_count": valid_count,
            "rejected_row_count": rejected_count,
        }
        if saved.status is SaveStatus.CREATED:
            return self._finish(
                context,
                DownloadStatus.TRADING_DATA,
                saved_path=saved.path,
                checksum=saved.checksum,
                **common,
            )
        if saved.status is SaveStatus.ALREADY_PRESENT:
            context.warnings.append(saved.message or "canonical file is unchanged")
            return self._finish(
                context,
                DownloadStatus.ALREADY_PRESENT,
                saved_path=saved.path,
                checksum=saved.checksum,
                **common,
            )
        if saved.status is SaveStatus.EXISTING_FILE_INVALID:
            return self._finish(
                context,
                DownloadStatus.EXISTING_FILE_INVALID,
                error=saved.message,
                **common,
            )
        return self._finish(
            context,
            DownloadStatus.FILE_CONFLICT,
            error=saved.message,
            **common,
        )


class SingleDateDownloader(_BaseSingleDateDownloader):
    """Synchronous D1 downloader, retained without local pre-fetch skipping."""

    def __init__(
        self,
        settings: Settings,
        client: PSXClient,
        *,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        super().__init__(settings, random_value=random_value)
        self.client = client
        self._sleep = sleep

    def _backoff(
        self,
        failed_attempt: int,
        retry_after_seconds: float | None = None,
    ) -> None:
        delay = self._retry_delay(failed_attempt, retry_after_seconds)
        logger.info(
            "retry backoff date_mode=sync attempt=%s delay_seconds=%.3f",
            failed_attempt,
            delay,
        )
        if delay > 0:
            self._sleep(delay)

    def download(self, requested_date: str) -> DownloadResult:
        context = _DownloadContext(requested_date=requested_date)
        try:
            parsed_date = validate_requested_date(requested_date)
        except ValueError as exc:
            return self._finish(context, DownloadStatus.INVALID_DATE, error=str(exc))

        for attempt in range(1, self.settings.retry_attempts + 1):
            context.attempts = attempt
            fetch_started = time.perf_counter()
            try:
                response = self.client.fetch(parsed_date)
            except PSXClientError as exc:
                context.network_ms += (time.perf_counter() - fetch_started) * 1000
                self._record_client_error(context, exc)
                if exc.retryable and attempt < self.settings.retry_attempts:
                    context.warnings.append(
                        f"attempt {attempt} failed: {exc}; retried"
                    )
                    self._backoff(attempt, exc.retry_after_seconds)
                    continue
                status = (
                    DownloadStatus.TEMPORARY_FAILURE
                    if exc.retryable
                    else DownloadStatus.HTTP_FAILURE
                )
                return self._finish(context, status, error=str(exc))

            context.network_ms += (time.perf_counter() - fetch_started) * 1000
            classification = self._classify_response(context, response)
            if classification is ContentClassification.EQUITY_ROWS:
                return self._process_equity_content(
                    context, parsed_date, response.content
                )
            if attempt < self.settings.retry_attempts:
                description = classification.value.lower().replace("_", " ")
                context.warnings.append(
                    f"attempt {attempt} returned {description}; retried"
                )
                self._backoff(attempt)
                continue
            return self._content_failure(context, classification)

        return self._finish(
            context,
            DownloadStatus.TEMPORARY_FAILURE,
            error="retry attempts exhausted without a usable response",
        )


class AsyncSingleDateDownloader(_BaseSingleDateDownloader):
    """Async D2 downloader sharing the exact D1 content-processing path."""

    def __init__(
        self,
        settings: Settings,
        client: AsyncPSXClient,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
        short_circuit_existing: bool = True,
    ) -> None:
        super().__init__(settings, random_value=random_value)
        self.client = client
        self._sleep = sleep
        self.short_circuit_existing = short_circuit_existing

    async def _backoff(
        self,
        failed_attempt: int,
        retry_after_seconds: float | None = None,
    ) -> None:
        delay = self._retry_delay(failed_attempt, retry_after_seconds)
        logger.info(
            "retry backoff date_mode=async attempt=%s delay_seconds=%.3f",
            failed_attempt,
            delay,
        )
        if delay > 0:
            await self._sleep(delay)

    async def download(self, requested_date: str) -> DownloadResult:
        context = _DownloadContext(requested_date=requested_date)
        try:
            parsed_date = validate_requested_date(requested_date)
        except ValueError as exc:
            return self._finish(context, DownloadStatus.INVALID_DATE, error=str(exc))

        if self.short_circuit_existing:
            inspection = inspect_existing_canonical_file(
                parsed_date,
                self.settings.raw_output_dir,
                self.settings.canonical_columns,
            )
            if inspection.valid:
                context.warnings.append(
                    "valid canonical file found locally; PSX request skipped"
                )
                return self._finish(
                    context,
                    DownloadStatus.ALREADY_PRESENT,
                    parsed_row_count=inspection.row_count,
                    valid_row_count=inspection.row_count,
                    saved_path=inspection.path,
                    checksum=inspection.checksum,
                    locally_skipped=True,
                )
            if inspection.exists:
                context.warnings.append(
                    "existing local file failed canonical validation; it will not "
                    "be overwritten"
                )

        for attempt in range(1, self.settings.retry_attempts + 1):
            context.attempts = attempt
            fetch_started = time.perf_counter()
            try:
                response = await self.client.fetch(parsed_date)
            except PSXClientError as exc:
                context.network_ms += (time.perf_counter() - fetch_started) * 1000
                self._record_client_error(context, exc)
                if exc.retryable and attempt < self.settings.retry_attempts:
                    context.warnings.append(
                        f"attempt {attempt} failed: {exc}; retried"
                    )
                    await self._backoff(attempt, exc.retry_after_seconds)
                    continue
                status = (
                    DownloadStatus.TEMPORARY_FAILURE
                    if exc.retryable
                    else DownloadStatus.HTTP_FAILURE
                )
                return self._finish(context, status, error=str(exc))

            context.network_ms += (time.perf_counter() - fetch_started) * 1000
            classification = self._classify_response(context, response)
            if classification is ContentClassification.EQUITY_ROWS:
                return self._process_equity_content(
                    context, parsed_date, response.content
                )
            if attempt < self.settings.retry_attempts:
                description = classification.value.lower().replace("_", " ")
                context.warnings.append(
                    f"attempt {attempt} returned {description}; retried"
                )
                await self._backoff(attempt)
                continue
            return self._content_failure(context, classification)

        return self._finish(
            context,
            DownloadStatus.TEMPORARY_FAILURE,
            error="retry attempts exhausted without a usable response",
        )
