"""High-level orchestration for one conservative PSX date download."""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

from .client import PSXClient, PSXClientError
from .config import Settings
from .exporter import save_canonical_csv
from .parser import classify_html, parse_equity_rows
from .state import (
    ContentClassification,
    DownloadResult,
    DownloadStatus,
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


class SingleDateDownloader:
    """Fetch, classify, parse, validate, and save one requested date."""

    def __init__(
        self,
        settings: Settings,
        client: PSXClient,
        *,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.settings = settings
        self.client = client
        self._sleep = sleep
        self._random_value = random_value

    def _backoff(self, failed_attempt: int) -> None:
        base = min(
            self.settings.retry_backoff_max_seconds,
            self.settings.retry_backoff_initial_seconds * (2 ** (failed_attempt - 1)),
        )
        delay = base + base * self.settings.retry_jitter_fraction * self._random_value()
        logger.info(
            "retry backoff attempt=%s delay_seconds=%.3f", failed_attempt, delay
        )
        if delay > 0:
            self._sleep(delay)

    def download(self, requested_date: str) -> DownloadResult:
        started = time.perf_counter()
        network_ms = 0.0
        parse_ms = 0.0
        validation_ms = 0.0
        save_ms = 0.0
        attempts = 0
        http_status: int | None = None
        response_bytes = 0
        warnings: list[str] = []

        def result(
            status: DownloadStatus,
            *,
            parsed_row_count: int = 0,
            valid_row_count: int = 0,
            rejected_row_count: int = 0,
            saved_path: Path | None = None,
            checksum: str | None = None,
            error: str | None = None,
        ) -> DownloadResult:
            completed = DownloadResult(
                requested_date=requested_date,
                status=status,
                http_status=http_status,
                attempts=attempts,
                response_bytes=response_bytes,
                parsed_row_count=parsed_row_count,
                valid_row_count=valid_row_count,
                rejected_row_count=rejected_row_count,
                elapsed_ms=(time.perf_counter() - started) * 1000,
                network_ms=network_ms,
                parse_ms=parse_ms,
                validation_ms=validation_ms,
                save_ms=save_ms,
                saved_path=saved_path,
                checksum=checksum,
                warnings=tuple(warnings),
                error=error,
            )
            logger.info(
                "PSX download complete date=%s status=%s http_status=%s "
                "attempts=%s response_bytes=%s parsed_rows=%s valid_rows=%s "
                "rejected_rows=%s duration_ms=%.2f",
                requested_date,
                completed.status.value,
                completed.http_status,
                completed.attempts,
                completed.response_bytes,
                completed.parsed_row_count,
                completed.valid_row_count,
                completed.rejected_row_count,
                completed.elapsed_ms,
            )
            return completed

        try:
            parsed_date = validate_requested_date(requested_date)
        except ValueError as exc:
            return result(DownloadStatus.INVALID_DATE, error=str(exc))

        response_content: bytes | None = None
        for attempt in range(1, self.settings.retry_attempts + 1):
            attempts = attempt
            fetch_started = time.perf_counter()
            try:
                response = self.client.fetch(parsed_date)
            except PSXClientError as exc:
                network_ms += (time.perf_counter() - fetch_started) * 1000
                http_status = exc.http_status
                response_bytes = exc.response_bytes
                logger.warning(
                    "PSX request failed date=%s attempt=%s retryable=%s error=%s",
                    requested_date,
                    attempt,
                    exc.retryable,
                    exc,
                )
                if exc.retryable and attempt < self.settings.retry_attempts:
                    warnings.append(f"attempt {attempt} failed: {exc}; retried")
                    self._backoff(attempt)
                    continue
                status = (
                    DownloadStatus.TEMPORARY_FAILURE
                    if exc.retryable
                    else DownloadStatus.HTTP_FAILURE
                )
                return result(status, error=str(exc))

            network_ms += (time.perf_counter() - fetch_started) * 1000
            http_status = response.status_code
            response_bytes = len(response.content)
            classification = classify_html(response.content)
            logger.info(
                "PSX content date=%s attempt=%s classification=%s response_bytes=%s",
                requested_date,
                attempt,
                classification.value,
                response_bytes,
            )
            if classification is ContentClassification.EQUITY_ROWS:
                response_content = response.content
                break

            retry_description = classification.value.lower().replace("_", " ")
            if attempt < self.settings.retry_attempts:
                warnings.append(
                    f"attempt {attempt} returned {retry_description}; retried"
                )
                self._backoff(attempt)
                continue

            if classification is ContentClassification.EMPTY_MARKET_RESPONSE:
                warnings.append(
                    "PSX returned a valid empty table on every completed content attempt; "
                    "this is not proof of a non-trading day"
                )
                return result(DownloadStatus.NON_TRADING_OR_EMPTY)
            return result(
                DownloadStatus.PARSE_FAILURE,
                error=f"unexpected response content: {retry_description}",
            )

        if response_content is None:
            return result(
                DownloadStatus.TEMPORARY_FAILURE,
                error="retry attempts exhausted without a usable response",
            )

        parse_started = time.perf_counter()
        try:
            parsed_rows = parse_equity_rows(response_content)
        except Exception as exc:
            parse_ms += (time.perf_counter() - parse_started) * 1000
            logger.exception("PSX parsing failed date=%s", requested_date)
            return result(DownloadStatus.PARSE_FAILURE, error=f"parse failed: {exc}")
        parse_ms += (time.perf_counter() - parse_started) * 1000

        validation_started = time.perf_counter()
        validation = validate_rows(parsed_rows)
        validation_ms += (time.perf_counter() - validation_started) * 1000
        parsed_count = len(parsed_rows)
        valid_count = len(validation.valid_rows)
        rejected_count = len(validation.rejected_rows)
        logger.info(
            "PSX rows date=%s parsed_rows=%s valid_rows=%s rejected_rows=%s",
            requested_date,
            parsed_count,
            valid_count,
            rejected_count,
        )
        if rejected_count:
            warnings.append(f"{rejected_count} malformed row(s) rejected")
            for rejected in validation.rejected_rows[:5]:
                label = rejected.symbol or f"row {rejected.row_index}"
                warnings.append(f"{label}: {'; '.join(rejected.reasons)}")

        if valid_count == 0:
            return result(
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
            save_ms += (time.perf_counter() - save_started) * 1000
            logger.exception("PSX CSV save failed date=%s", requested_date)
            return result(
                DownloadStatus.SAVE_FAILURE,
                parsed_row_count=parsed_count,
                valid_row_count=valid_count,
                rejected_row_count=rejected_count,
                error=f"failed to save canonical CSV: {exc}",
            )
        save_ms += (time.perf_counter() - save_started) * 1000

        common = {
            "parsed_row_count": parsed_count,
            "valid_row_count": valid_count,
            "rejected_row_count": rejected_count,
        }
        if saved.status is SaveStatus.CREATED:
            final = result(
                DownloadStatus.TRADING_DATA,
                saved_path=saved.path,
                checksum=saved.checksum,
                **common,
            )
        elif saved.status is SaveStatus.ALREADY_PRESENT:
            warnings.append(saved.message or "canonical file is unchanged")
            final = result(
                DownloadStatus.ALREADY_PRESENT,
                saved_path=saved.path,
                checksum=saved.checksum,
                **common,
            )
        elif saved.status is SaveStatus.EXISTING_FILE_INVALID:
            final = result(
                DownloadStatus.EXISTING_FILE_INVALID,
                error=saved.message,
                **common,
            )
        else:
            final = result(
                DownloadStatus.FILE_CONFLICT,
                error=saved.message,
                **common,
            )

        return final
