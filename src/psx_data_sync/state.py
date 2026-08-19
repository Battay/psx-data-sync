"""Domain types shared by the D1 and D2 download pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path


class ContentClassification(StrEnum):
    EQUITY_ROWS = "EQUITY_ROWS"
    EMPTY_MARKET_RESPONSE = "EMPTY_MARKET_RESPONSE"
    MALFORMED_HTML = "MALFORMED_HTML"
    NON_HTML = "NON_HTML"


class DownloadStatus(StrEnum):
    TRADING_DATA = "TRADING_DATA"
    EMPTY_MARKET_RESPONSE = "EMPTY_MARKET_RESPONSE"
    NON_TRADING_OR_EMPTY = "NON_TRADING_OR_EMPTY"
    TEMPORARY_FAILURE = "TEMPORARY_FAILURE"
    HTTP_FAILURE = "HTTP_FAILURE"
    PARSE_FAILURE = "PARSE_FAILURE"
    VALIDATION_FAILURE = "VALIDATION_FAILURE"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    EXISTING_FILE_INVALID = "EXISTING_FILE_INVALID"
    FILE_CONFLICT = "FILE_CONFLICT"
    SAVE_FAILURE = "SAVE_FAILURE"
    INVALID_DATE = "INVALID_DATE"


class ClientFailureKind(StrEnum):
    TIMEOUT = "TIMEOUT"
    CONNECTION = "CONNECTION"
    HTTP = "HTTP"
    EMPTY_BODY = "EMPTY_BODY"


class SaveStatus(StrEnum):
    CREATED = "CREATED"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    EXISTING_FILE_INVALID = "EXISTING_FILE_INVALID"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class FetchResponse:
    status_code: int
    content: bytes


@dataclass(frozen=True, slots=True)
class ParsedEquityRow:
    row_index: int
    raw_values: tuple[str, ...]
    symbol: str
    ldcp: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    change: Decimal | None
    change_percent: Decimal | None
    volume: int | None
    parse_errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidEquityRow:
    row_index: int
    symbol: str
    ldcp: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    change: Decimal
    change_percent: Decimal
    volume: int


@dataclass(frozen=True, slots=True)
class RejectedRow:
    row_index: int
    symbol: str
    raw_values: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid_rows: tuple[ValidEquityRow, ...]
    rejected_rows: tuple[RejectedRow, ...]


@dataclass(frozen=True, slots=True)
class SaveResult:
    status: SaveStatus
    path: Path
    checksum: str | None
    message: str | None = None


@dataclass(frozen=True, slots=True)
class DownloadResult:
    requested_date: str
    status: DownloadStatus
    http_status: int | None = None
    attempts: int = 0
    response_bytes: int = 0
    cumulative_response_bytes: int = 0
    parsed_row_count: int = 0
    valid_row_count: int = 0
    rejected_row_count: int = 0
    elapsed_ms: float = 0.0
    network_ms: float = 0.0
    parse_ms: float = 0.0
    validation_ms: float = 0.0
    save_ms: float = 0.0
    saved_path: Path | None = None
    checksum: str | None = None
    rate_limit_count: int = 0
    locally_skipped: bool = False
    warnings: tuple[str, ...] = ()
    error: str | None = None

    @property
    def successful(self) -> bool:
        return self.status in {
            DownloadStatus.TRADING_DATA,
            DownloadStatus.ALREADY_PRESENT,
        }

    @property
    def transferred_response_bytes(self) -> int:
        """Include every HTTP attempt while preserving the D1 final-size field."""

        return self.cumulative_response_bytes or self.response_bytes


@dataclass(frozen=True, slots=True)
class ExistingFileInspection:
    path: Path
    exists: bool
    valid: bool
    row_count: int = 0
    checksum: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class RangeDownloadResult:
    start_date: str
    end_date: str
    requested_dates: tuple[str, ...]
    workers: int
    total_duration_ms: float
    results: tuple[DownloadResult, ...]
    counts_by_status: dict[DownloadStatus, int]
    total_parsed_rows: int
    total_valid_rows: int
    total_rejected_rows: int
    total_response_bytes: int
    total_retries: int
    rate_limit_occurrences: int
    network_fetched_dates: int
    locally_skipped_dates: int
    verified_successful_dates: int
    failed_dates: tuple[str, ...]
    unresolved_empty_dates: tuple[str, ...]
    average_per_date_duration_ms: float
    dates_per_second: float
    verified_dates_per_second: float
    network_dates_per_second: float
    rows_per_second: float
    warnings: tuple[str, ...] = ()

    @property
    def requested_count(self) -> int:
        return len(self.requested_dates)

    @property
    def has_failures(self) -> bool:
        return bool(self.failed_dates)

    @property
    def has_unresolved_empty(self) -> bool:
        return bool(self.unresolved_empty_dates)


@dataclass(frozen=True, slots=True)
class BenchmarkMetrics:
    workers: int
    requested_dates: int
    total_duration_ms: float
    dates_per_second: float
    verified_dates_per_second: float
    network_dates_per_second: float
    rows_per_second: float
    retries: int
    failures: int
    response_bytes: int
