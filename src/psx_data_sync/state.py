"""Domain types shared by the D1 download pipeline."""

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
    warnings: tuple[str, ...] = ()
    error: str | None = None

    @property
    def successful(self) -> bool:
        return self.status in {
            DownloadStatus.TRADING_DATA,
            DownloadStatus.ALREADY_PRESENT,
        }
