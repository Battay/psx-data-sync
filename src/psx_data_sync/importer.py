"""Network-free bulk importer for historical canonical CSV files."""

from __future__ import annotations

import csv
import io
import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import StrEnum
from pathlib import Path

from .exporter import (
    _atomic_create_without_overwrite,
    canonical_csv_bytes,
    inspect_canonical_csv_file,
    sha256_bytes,
)
from .state import ParsedEquityRow
from .state_db import StateRepository
from .validator import validate_rows

logger = logging.getLogger(__name__)

MARKET_FILE_PATTERN = re.compile(r"^market_(\d{4}-\d{2}-\d{2})\.csv$")

LEGACY_COLUMNS: tuple[str, ...] = (
    "symbol",
    "date",
    "ldcp",
    "open",
    "high",
    "low",
    "close",
    "change",
    "change_percent",
    "volume",
)

LEGACY_NUMERIC_PATTERN = re.compile(
    r"^[+-]?(?:(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
)

NUMERIC_FIELDS: tuple[str, ...] = (
    "ldcp",
    "open",
    "high",
    "low",
    "close",
    "change",
    "change_percent",
)


def parse_legacy_decimal(
    raw: str, field_name: str
) -> tuple[Decimal | None, str | None]:
    """Parse numeric fields supporting finite decimals and scientific notation."""

    value = raw.strip()
    if not value:
        return None, f"{field_name} is empty"
    if value.casefold() == "null":
        return None, f"{field_name} is null"

    if field_name == "change_percent" and value.endswith("%"):
        value = value[:-1].strip()
        if not value:
            return None, "change_percent is empty"

    if LEGACY_NUMERIC_PATTERN.fullmatch(value) is None:
        return None, f"{field_name} is not numeric: {value!r}"

    normalized = value.replace(",", "")
    try:
        parsed = Decimal(normalized)
    except InvalidOperation:
        return None, f"{field_name} is not numeric: {value!r}"
    if not parsed.is_finite():
        return None, f"{field_name} is not finite"

    if field_name in ("change", "change_percent"):
        parsed = parsed.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    return parsed, None


def parse_legacy_volume(raw: str) -> tuple[int | None, str | None]:
    value, error = parse_legacy_decimal(raw, "volume")
    if error is not None or value is None:
        return None, error
    if value != value.to_integral_value():
        return None, "volume must be an integer"
    return int(value), None


def parse_legacy_field_values(
    values: tuple[str, ...], row_index: int
) -> ParsedEquityRow:
    """Parse one 9-field legacy tuple (without date) while retaining errors."""

    symbol = values[0] if values else ""
    if len(values) != 9:
        return ParsedEquityRow(
            row_index=row_index,
            raw_values=values,
            symbol=symbol,
            ldcp=None,
            open=None,
            high=None,
            low=None,
            close=None,
            change=None,
            change_percent=None,
            volume=None,
            parse_errors=(f"expected 9 fields, found {len(values)}",),
        )

    parsed_numbers: list[Decimal | None] = []
    errors: list[str] = []
    for field_name, raw in zip(NUMERIC_FIELDS, values[1:8], strict=True):
        number, error = parse_legacy_decimal(raw, field_name)
        parsed_numbers.append(number)
        if error is not None:
            errors.append(error)

    volume, vol_error = parse_legacy_volume(values[8])
    if vol_error is not None:
        errors.append(vol_error)

    return ParsedEquityRow(
        row_index=row_index,
        raw_values=values,
        symbol=symbol,
        ldcp=parsed_numbers[0],
        open=parsed_numbers[1],
        high=parsed_numbers[2],
        low=parsed_numbers[3],
        close=parsed_numbers[4],
        change=parsed_numbers[5],
        change_percent=parsed_numbers[6],
        volume=volume,
        parse_errors=tuple(errors),
    )


class LocalImportAction(StrEnum):
    """Decision for one candidate source CSV file."""

    IMPORT = "IMPORT"
    ALREADY_PRESENT = "ALREADY_PRESENT"
    INVALID_SOURCE = "INVALID_SOURCE"
    CONFLICT = "CONFLICT"
    UNSUPPORTED_FILENAME = "UNSUPPORTED_FILENAME"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class LocalFileImportResult:
    """Result of evaluating/importing one local CSV file."""

    source_path: Path
    market_date: str | None
    action: LocalImportAction
    valid: bool
    row_count: int | None
    rejected_row_count: int
    source_checksum: str | None
    destination_path: Path | None
    destination_checksum: str | None
    imported: bool
    warnings: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True, slots=True)
class BatchImportResult:
    """Result of evaluating/importing a directory of historical CSV files."""

    source_dir: Path
    destination_dir: Path
    discovered_count: int
    candidate_count: int
    importable_count: int
    imported_count: int
    already_present_count: int
    invalid_count: int
    conflict_count: int
    unsupported_count: int
    failed_count: int
    dry_run: bool
    duration_ms: float
    results: tuple[LocalFileImportResult, ...]


@dataclass(frozen=True, slots=True)
class LegacyFileInspection:
    """Outcome of inspecting a legacy 10-column CSV source file."""

    exists: bool
    valid: bool
    is_legacy: bool
    row_count: int | None = None
    rejected_row_count: int = 0
    canonical_bytes: bytes | None = None
    canonical_checksum: str | None = None
    error: str | None = None


def inspect_legacy_csv_file(
    path: Path,
    expected_market_date: str,
) -> LegacyFileInspection:
    """Inspect a legacy 10-column CSV file and produce canonical 9-column bytes if valid."""

    if not path.exists() or not path.is_file():
        return LegacyFileInspection(exists=False, valid=False, is_legacy=False)

    try:
        content = path.read_bytes()
        text = content.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=False,
            error=f"file cannot be read as UTF-8: {exc}",
        )

    try:
        records = list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=False,
            error=f"file is not valid CSV: {exc}",
        )

    if not records:
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=False,
            error="file is empty",
        )

    header = tuple(field.strip() for field in records[0])
    if header != LEGACY_COLUMNS:
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=False,
            error="file header does not match canonical or legacy schema",
        )

    if len(records) == 1:
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=True,
            error="legacy file contains no data rows",
        )

    parsed_rows: list[ParsedEquityRow] = []
    for row_index, record in enumerate(records[1:], start=1):
        clean_record = tuple(field.strip() for field in record)
        if len(clean_record) != 10:
            return LegacyFileInspection(
                exists=True,
                valid=False,
                is_legacy=True,
                error=f"row {row_index} has {len(clean_record)} fields, expected 10",
            )
        (
            symbol,
            row_date,
            ldcp,
            open_p,
            high,
            low,
            close,
            change,
            change_pct,
            volume,
        ) = clean_record
        if row_date != expected_market_date:
            return LegacyFileInspection(
                exists=True,
                valid=False,
                is_legacy=True,
                error=(
                    f"row {row_index} date {row_date!r} does not match market"
                    f" date {expected_market_date!r}"
                ),
            )
        nine_tuple = (
            symbol,
            ldcp,
            open_p,
            high,
            low,
            close,
            change,
            change_pct,
            volume,
        )
        parsed_rows.append(parse_legacy_field_values(nine_tuple, row_index))

    validation = validate_rows(parsed_rows)
    rejected_count = len(validation.rejected_rows)

    if not validation.valid_rows:
        first_err = (
            f"; ".join(validation.rejected_rows[0].reasons)
            if validation.rejected_rows
            else "legacy file contains 0 valid rows"
        )
        return LegacyFileInspection(
            exists=True,
            valid=False,
            is_legacy=True,
            rejected_row_count=rejected_count,
            error=f"legacy file contains 0 valid rows ({first_err})",
        )

    canonical_bytes = canonical_csv_bytes(validation.valid_rows)
    canonical_checksum = sha256_bytes(canonical_bytes)

    return LegacyFileInspection(
        exists=True,
        valid=True,
        is_legacy=True,
        row_count=len(validation.valid_rows),
        rejected_row_count=rejected_count,
        canonical_bytes=canonical_bytes,
        canonical_checksum=canonical_checksum,
    )


def import_local_csv_file(
    repository: StateRepository,
    source_path: Path,
    *,
    destination_dir: Path | None = None,
    dry_run: bool = False,
) -> LocalFileImportResult:
    """Evaluate and optionally import a single local CSV file."""

    default_dest = (
        getattr(repository, "raw_output_dir", None)
        or Settings.from_env().raw_output_dir
    )
    dest_dir = (destination_dir or default_dest).resolve()

    match = MARKET_FILE_PATTERN.fullmatch(source_path.name)
    if match is None:
        return LocalFileImportResult(
            source_path=source_path,
            market_date=None,
            action=LocalImportAction.UNSUPPORTED_FILENAME,
            valid=False,
            row_count=None,
            rejected_row_count=0,
            source_checksum=None,
            destination_path=None,
            destination_checksum=None,
            imported=False,
            error="filename does not match market_YYYY-MM-DD.csv pattern",
        )

    date_text = match.group(1)
    try:
        date.fromisoformat(date_text)
    except ValueError:
        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.INVALID_SOURCE,
            valid=False,
            row_count=None,
            rejected_row_count=0,
            source_checksum=None,
            destination_path=None,
            destination_checksum=None,
            imported=False,
            error="filename contains an invalid calendar date",
        )

    # Inspect source CSV (first try canonical 9-column, then fallback to legacy 10-column)
    source_inspection = inspect_canonical_csv_file(source_path)
    canonical_bytes_to_write: bytes | None = None
    rejected_row_count = 0

    if (
        source_inspection.exists
        and source_inspection.valid
        and source_inspection.checksum is not None
        and source_inspection.row_count is not None
        and source_inspection.row_count > 0
    ):
        source_row_count = source_inspection.row_count
        source_checksum = source_inspection.checksum
        canonical_bytes_to_write = source_path.read_bytes()
    else:
        legacy_inspection = inspect_legacy_csv_file(source_path, date_text)
        if (
            legacy_inspection.is_legacy
            and legacy_inspection.valid
            and legacy_inspection.canonical_bytes is not None
            and legacy_inspection.canonical_checksum is not None
            and legacy_inspection.row_count is not None
        ):
            source_row_count = legacy_inspection.row_count
            rejected_row_count = legacy_inspection.rejected_row_count
            source_checksum = legacy_inspection.canonical_checksum
            canonical_bytes_to_write = legacy_inspection.canonical_bytes
        else:
            err_msg = (
                legacy_inspection.error
                if legacy_inspection.is_legacy
                else source_inspection.error or "invalid canonical CSV file"
            )
            return LocalFileImportResult(
                source_path=source_path,
                market_date=date_text,
                action=LocalImportAction.INVALID_SOURCE,
                valid=False,
                row_count=source_inspection.row_count if source_inspection.exists else None,
                rejected_row_count=legacy_inspection.rejected_row_count if legacy_inspection.is_legacy else 0,
                source_checksum=None,
                destination_path=None,
                destination_checksum=None,
                imported=False,
                error=err_msg,
            )

    # Source file is valid! Determine destination path.
    dest_path = (dest_dir / f"market_{date_text}.csv").resolve()

    # Special case: source and destination are the exact same file path
    if source_path == dest_path:
        existing_state = repository.get_date_state(date_text)
        state_conflict = (
            existing_state is not None
            and existing_state.csv_checksum_sha256 is not None
            and existing_state.csv_checksum_sha256 != source_checksum
        )
        if state_conflict:
            return LocalFileImportResult(
                source_path=source_path,
                market_date=date_text,
                action=LocalImportAction.CONFLICT,
                valid=True,
                row_count=source_row_count,
                rejected_row_count=rejected_row_count,
                source_checksum=source_checksum,
                destination_path=dest_path,
                destination_checksum=source_checksum,
                imported=False,
                error=f"database state checksum mismatch for date {date_text}",
            )

        if not dry_run:
            repository.index_local_file(dest_path)

        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.ALREADY_PRESENT,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path,
            destination_checksum=source_checksum,
            imported=False,
        )

    # Check destination file and DB state
    dest_exists = dest_path.exists()
    dest_inspection = inspect_canonical_csv_file(dest_path) if dest_exists else None
    dest_checksum = (
        dest_inspection.checksum if (dest_inspection and dest_inspection.exists) else None
    )

    existing_state = repository.get_date_state(date_text)
    state_checksum = (
        existing_state.csv_checksum_sha256
        if (existing_state and existing_state.csv_checksum_sha256)
        else None
    )

    state_conflict = (
        state_checksum is not None and state_checksum != source_checksum
    )
    dest_conflict = (
        dest_exists
        and (
            dest_inspection is None
            or not dest_inspection.valid
            or dest_inspection.checksum != source_checksum
        )
    )

    if dest_conflict or state_conflict:
        err_msg = (
            f"destination file already exists and differs: {dest_path}"
            if dest_conflict
            else f"conflicting verified state exists for date {date_text}"
        )
        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.CONFLICT,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path if dest_exists else None,
            destination_checksum=dest_checksum,
            imported=False,
            error=err_msg,
        )

    if (
        dest_exists
        and dest_inspection is not None
        and dest_inspection.valid
        and dest_inspection.checksum == source_checksum
    ):
        if not dry_run:
            repository.index_local_file(dest_path)
        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.ALREADY_PRESENT,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path,
            destination_checksum=dest_checksum,
            imported=False,
        )

    # Destination does not exist. Action: IMPORT
    if dry_run:
        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.IMPORT,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path,
            destination_checksum=None,
            imported=False,
        )

    # Apply mode: Copy & index atomically
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        published = _atomic_create_without_overwrite(
            dest_path, canonical_bytes_to_write
        )
        if not published:
            dest_inspection_now = inspect_canonical_csv_file(dest_path)
            if (
                dest_inspection_now.valid
                and dest_inspection_now.checksum == source_checksum
            ):
                repository.index_local_file(dest_path)
                return LocalFileImportResult(
                    source_path=source_path,
                    market_date=date_text,
                    action=LocalImportAction.ALREADY_PRESENT,
                    valid=True,
                    row_count=source_row_count,
                    rejected_row_count=rejected_row_count,
                    source_checksum=source_checksum,
                    destination_path=dest_path,
                    destination_checksum=dest_inspection_now.checksum,
                    imported=False,
                )
            else:
                return LocalFileImportResult(
                    source_path=source_path,
                    market_date=date_text,
                    action=LocalImportAction.CONFLICT,
                    valid=True,
                    row_count=source_row_count,
                    rejected_row_count=rejected_row_count,
                    source_checksum=source_checksum,
                    destination_path=dest_path,
                    destination_checksum=(
                        dest_inspection_now.checksum
                        if dest_inspection_now.exists
                        else None
                    ),
                    imported=False,
                    error=(
                        "destination file created concurrently with"
                        " conflicting content"
                    ),
                )

        dest_inspection_now = inspect_canonical_csv_file(dest_path)
        if (
            not dest_inspection_now.valid
            or dest_inspection_now.checksum != source_checksum
            or dest_inspection_now.row_count != source_row_count
        ):
            dest_path.unlink(missing_ok=True)
            return LocalFileImportResult(
                source_path=source_path,
                market_date=date_text,
                action=LocalImportAction.FAILED,
                valid=True,
                row_count=source_row_count,
                rejected_row_count=rejected_row_count,
                source_checksum=source_checksum,
                destination_path=dest_path,
                destination_checksum=(
                    dest_inspection_now.checksum
                    if dest_inspection_now.exists
                    else None
                ),
                imported=False,
                error="published destination verification failed",
            )

        repository.index_local_file(dest_path)

        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.IMPORT,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path,
            destination_checksum=dest_inspection_now.checksum,
            imported=True,
        )
    except Exception as exc:
        logger.exception(f"failed to import local file {source_path}")
        return LocalFileImportResult(
            source_path=source_path,
            market_date=date_text,
            action=LocalImportAction.FAILED,
            valid=True,
            row_count=source_row_count,
            rejected_row_count=rejected_row_count,
            source_checksum=source_checksum,
            destination_path=dest_path,
            destination_checksum=None,
            imported=False,
            error=str(exc),
        )


def import_local_csv_directory(
    repository: StateRepository,
    source_dir: Path,
    *,
    destination_dir: Path | None = None,
    dry_run: bool = False,
    recursive: bool = False,
) -> BatchImportResult:
    """Scan and import historical canonical CSV files from a directory."""

    source_dir = Path(source_dir).resolve()
    default_dest = (
        getattr(repository, "raw_output_dir", None)
        or Settings.from_env().raw_output_dir
    )
    dest_dir = (destination_dir or default_dest).resolve()

    if not source_dir.exists() or not source_dir.is_dir():
        raise FileNotFoundError(
            f"source directory does not exist or is not a directory: {source_dir}"
        )

    started = time.perf_counter()

    if recursive:
        candidates = sorted(p for p in source_dir.rglob("*.csv") if p.is_file())
    else:
        candidates = sorted(p for p in source_dir.glob("*.csv") if p.is_file())

    results: list[LocalFileImportResult] = []
    for candidate in candidates:
        res = import_local_csv_file(
            repository,
            candidate,
            destination_dir=dest_dir,
            dry_run=dry_run,
        )
        results.append(res)

    duration_ms = (time.perf_counter() - started) * 1000.0

    discovered = len(results)
    unsupported = sum(
        1 for r in results if r.action is LocalImportAction.UNSUPPORTED_FILENAME
    )
    candidate_count = discovered - unsupported
    invalid_count = sum(
        1 for r in results if r.action is LocalImportAction.INVALID_SOURCE
    )
    conflict_count = sum(
        1 for r in results if r.action is LocalImportAction.CONFLICT
    )
    already_present_count = sum(
        1 for r in results if r.action is LocalImportAction.ALREADY_PRESENT
    )
    failed_count = sum(
        1 for r in results if r.action is LocalImportAction.FAILED
    )
    imported_count = sum(1 for r in results if r.imported)
    importable_count = sum(
        1 for r in results if r.action is LocalImportAction.IMPORT
    )

    return BatchImportResult(
        source_dir=source_dir,
        destination_dir=dest_dir,
        discovered_count=discovered,
        candidate_count=candidate_count,
        importable_count=importable_count,
        imported_count=imported_count,
        already_present_count=already_present_count,
        invalid_count=invalid_count,
        conflict_count=conflict_count,
        unsupported_count=unsupported,
        failed_count=failed_count,
        dry_run=dry_run,
        duration_ms=duration_ms,
        results=tuple(results),
    )
