"""Deterministic and atomic canonical CSV persistence."""

from __future__ import annotations

import csv
import hashlib
import io
import os
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from .config import CANONICAL_COLUMNS
from .parser import parse_field_values
from .state import (
    ExistingFileInspection,
    SaveResult,
    SaveStatus,
    ValidEquityRow,
)
from .validator import validate_rows


class StagedPromotionStatus(StrEnum):
    """Outcome of a guarded staged-artifact promotion."""

    PROMOTED = "PROMOTED"
    STAGED_FILE_INVALID = "STAGED_FILE_INVALID"
    HISTORICAL_MISMATCH = "HISTORICAL_MISMATCH"
    DESTINATION_ALREADY_EXISTS = "DESTINATION_ALREADY_EXISTS"
    POLICY_REJECTED = "POLICY_REJECTED"


@dataclass(frozen=True, slots=True)
class StagedPromotionResult:
    """Audit-friendly result from a no-clobber staged promotion."""

    status: StagedPromotionStatus
    staged_path: Path
    destination_path: Path
    row_count: int
    checksum: str | None
    message: str

    @property
    def promoted(self) -> bool:
        return self.status is StagedPromotionStatus.PROMOTED


def _format_decimal(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"-0", ""} else rendered


def canonical_csv_bytes(
    rows: Iterable[ValidEquityRow],
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> bytes:
    """Render stable UTF-8 CSV bytes in canonical symbol order."""

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    for row in sorted(rows, key=lambda item: (item.symbol.casefold(), item.symbol)):
        writer.writerow(
            (
                row.symbol,
                _format_decimal(row.ldcp),
                _format_decimal(row.open),
                _format_decimal(row.high),
                _format_decimal(row.low),
                _format_decimal(row.close),
                _format_decimal(row.change),
                _format_decimal(row.change_percent),
                str(row.volume),
            )
        )
    return output.getvalue().encode("utf-8")


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _validated_existing_rows(
    content: bytes,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> tuple[tuple[ValidEquityRow, ...] | None, str | None]:

    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None, "existing file is not valid UTF-8"

    try:
        records = list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        return None, f"existing file is not valid CSV: {exc}"

    if not records:
        return None, "existing file is empty"
    if tuple(records[0]) != tuple(columns):
        return None, "existing file has a non-canonical header"
    if len(records) == 1:
        return None, "existing file contains no data rows"

    parsed_rows = tuple(
        parse_field_values(record, row_index)
        for row_index, record in enumerate(records[1:], start=1)
    )
    validation = validate_rows(parsed_rows)
    if validation.rejected_rows:
        first = validation.rejected_rows[0]
        return (
            None,
            f"existing file row {first.row_index} is invalid: "
            + "; ".join(first.reasons),
        )
    return validation.valid_rows, None


def validate_existing_csv(
    content: bytes,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> tuple[bool, str | None]:
    """Validate schema and every row of an existing canonical candidate."""

    rows, error = _validated_existing_rows(content, columns)
    return rows is not None, error


def _inspect_canonical_content(
    path: Path,
    content: bytes,
    columns: Sequence[str],
) -> ExistingFileInspection:
    checksum = sha256_bytes(content)
    rows, error = _validated_existing_rows(content, columns)
    if rows is None:
        return ExistingFileInspection(
            path=path,
            exists=True,
            valid=False,
            checksum=checksum,
            error=error,
        )

    if canonical_csv_bytes(rows, columns) != content:
        return ExistingFileInspection(
            path=path,
            exists=True,
            valid=False,
            row_count=len(rows),
            checksum=checksum,
            error="file is valid CSV but not canonical deterministic content",
        )
    return ExistingFileInspection(
        path=path,
        exists=True,
        valid=True,
        row_count=len(rows),
        checksum=checksum,
    )


def _read_and_inspect_canonical_file(
    path: Path,
    columns: Sequence[str],
) -> tuple[ExistingFileInspection, bytes | None]:
    if not os.path.lexists(path):
        return ExistingFileInspection(path=path, exists=False, valid=False), None
    try:
        content = path.read_bytes()
    except OSError as exc:
        return (
            ExistingFileInspection(
                path=path,
                exists=os.path.lexists(path),
                valid=False,
                error=f"file cannot be read: {exc}",
            ),
            None,
        )
    return _inspect_canonical_content(path, content, columns), content


def inspect_canonical_csv_file(
    path: Path,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> ExistingFileInspection:
    """Inspect any canonical CSV candidate without modifying it.

    The returned checksum is the checksum of the exact bytes on disk. A file is
    valid only when its schema and rows pass the D1 rules *and* re-rendering the
    parsed rows produces those exact deterministic bytes.
    """

    inspection, _ = _read_and_inspect_canonical_file(Path(path), columns)
    return inspection


def inspect_existing_canonical_file(
    requested_date: date,
    output_dir: Path,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> ExistingFileInspection:
    """Validate and checksum a local D1 file for a network-free D2 skip."""

    path = output_dir / f"market_{requested_date.isoformat()}.csv"
    return inspect_canonical_csv_file(path, columns)


def _fsync_directory(path: Path) -> None:
    try:
        directory_fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(directory_fd)
        except OSError:
            # The file itself is already durable; not all platforms permit
            # fsync on directory descriptors.
            pass
    finally:
        os.close(directory_fd)


def _atomic_create_without_overwrite(path: Path, content: bytes) -> bool:
    """Atomically create ``path`` and return false if it already exists."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".promotion.tmp",
    )
    temporary_path = Path(temporary_name)
    linked = False
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # A hard-link create is atomic and fails with EEXIST. Unlike
            # os.replace(), it can never overwrite a concurrent winner.
            os.link(temporary_path, path)
        except FileExistsError:
            return False
        linked = True
        _fsync_directory(path.parent)
        return True
    finally:
        temporary_path.unlink(missing_ok=True)
        if linked:
            _fsync_directory(path.parent)


def promote_staged_csv_if_safe(
    staged_path: Path,
    destination_path: Path,
    *,
    expected_checksum: str | None = None,
    expected_row_count: int | None = None,
    allow_new: bool = False,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> StagedPromotionResult:
    """Promote a staged canonical CSV under the D4 no-overwrite policy.

    Historical repair requires both the prior checksum and row count and will
    proceed only when both match the staged byte snapshot exactly. A date with
    no prior artifact identity must opt into ``allow_new`` explicitly. The
    destination is created atomically without replacement, and the staged file
    is never moved, linked, or removed.
    """

    staged_path = Path(staged_path)
    destination_path = Path(destination_path)
    inspection, content = _read_and_inspect_canonical_file(staged_path, columns)
    if not inspection.valid or content is None:
        return StagedPromotionResult(
            status=StagedPromotionStatus.STAGED_FILE_INVALID,
            staged_path=staged_path,
            destination_path=destination_path,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            message=inspection.error or "staged canonical CSV is missing",
        )

    has_expected_checksum = expected_checksum is not None
    has_expected_row_count = expected_row_count is not None
    if has_expected_checksum != has_expected_row_count:
        return StagedPromotionResult(
            status=StagedPromotionStatus.POLICY_REJECTED,
            staged_path=staged_path,
            destination_path=destination_path,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            message=(
                "historical repair requires both expected checksum and "
                "expected row count"
            ),
        )

    if has_expected_checksum:
        if (
            inspection.checksum != expected_checksum
            or inspection.row_count != expected_row_count
        ):
            return StagedPromotionResult(
                status=StagedPromotionStatus.HISTORICAL_MISMATCH,
                staged_path=staged_path,
                destination_path=destination_path,
                row_count=inspection.row_count,
                checksum=inspection.checksum,
                message=(
                    "staged artifact does not match historical identity "
                    f"(expected SHA-256 {expected_checksum}, rows "
                    f"{expected_row_count}; observed SHA-256 "
                    f"{inspection.checksum}, rows {inspection.row_count})"
                ),
            )
    elif not allow_new:
        return StagedPromotionResult(
            status=StagedPromotionStatus.POLICY_REJECTED,
            staged_path=staged_path,
            destination_path=destination_path,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            message=(
                "promotion without historical identity requires allow_new=True"
            ),
        )

    if os.path.lexists(destination_path):
        return StagedPromotionResult(
            status=StagedPromotionStatus.DESTINATION_ALREADY_EXISTS,
            staged_path=staged_path,
            destination_path=destination_path,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            message="destination already exists; no file was overwritten",
        )

    if not _atomic_create_without_overwrite(destination_path, content):
        return StagedPromotionResult(
            status=StagedPromotionStatus.DESTINATION_ALREADY_EXISTS,
            staged_path=staged_path,
            destination_path=destination_path,
            row_count=inspection.row_count,
            checksum=inspection.checksum,
            message=(
                "destination was created concurrently; no file was overwritten"
            ),
        )

    return StagedPromotionResult(
        status=StagedPromotionStatus.PROMOTED,
        staged_path=staged_path,
        destination_path=destination_path,
        row_count=inspection.row_count,
        checksum=inspection.checksum,
        message="staged canonical CSV promoted without overwriting existing data",
    )


def promote_staged_canonical_file(
    staged_path: Path,
    destination_path: Path,
    *,
    expected_checksum: str | None = None,
    expected_row_count: int | None = None,
    allow_new_gap: bool = False,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> StagedPromotionResult:
    """Compatibility spelling for :func:`promote_staged_csv_if_safe`."""

    return promote_staged_csv_if_safe(
        staged_path,
        destination_path,
        expected_checksum=expected_checksum,
        expected_row_count=expected_row_count,
        allow_new=allow_new_gap,
        columns=columns,
    )


def save_canonical_csv(
    rows: Iterable[ValidEquityRow],
    requested_date: date,
    output_dir: Path,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> SaveResult:
    """Atomically create canonical content without replacing any existing data."""

    materialized_rows = tuple(rows)
    if not materialized_rows:
        raise ValueError("refusing to save a canonical CSV without valid rows")
    content = canonical_csv_bytes(materialized_rows, columns)
    checksum = sha256_bytes(content)
    path = output_dir / f"market_{requested_date.isoformat()}.csv"

    def existing_result() -> SaveResult | None:
        if not os.path.lexists(path):
            return None
        try:
            existing_content = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            return SaveResult(
                status=SaveStatus.EXISTING_FILE_INVALID,
                path=path,
                checksum=None,
                message=f"existing file cannot be read: {exc}",
            )
        valid, message = validate_existing_csv(existing_content, columns)
        if not valid:
            return SaveResult(
                status=SaveStatus.EXISTING_FILE_INVALID,
                path=path,
                checksum=None,
                message=message,
            )
        existing_checksum = sha256_bytes(existing_content)
        if existing_content == content:
            return SaveResult(
                status=SaveStatus.ALREADY_PRESENT,
                path=path,
                checksum=existing_checksum,
                message="canonical file is already present and unchanged",
            )
        return SaveResult(
            status=SaveStatus.CONFLICT,
            path=path,
            checksum=None,
            message=(
                "existing valid file differs from downloaded canonical content "
                f"(existing SHA-256 {existing_checksum}, incoming SHA-256 {checksum})"
            ),
        )

    # The initial inspection is only an optimization.  The hard-link create is
    # the actual no-clobber boundary; if another process wins the race, inspect
    # its bytes and report the appropriate local outcome.
    for _ in range(3):
        existing = existing_result()
        if existing is not None:
            return existing
        if _atomic_create_without_overwrite(path, content):
            return SaveResult(
                status=SaveStatus.CREATED,
                path=path,
                checksum=checksum,
            )
    existing = existing_result()
    if existing is not None:
        return existing
    raise OSError(f"canonical destination changed repeatedly while saving {path}")
