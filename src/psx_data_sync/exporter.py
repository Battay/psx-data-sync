"""Deterministic and atomic canonical CSV persistence."""

from __future__ import annotations

import csv
import hashlib
import io
import os
import tempfile
from collections.abc import Iterable, Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path

from .config import CANONICAL_COLUMNS
from .parser import parse_field_values
from .state import SaveResult, SaveStatus, ValidEquityRow
from .validator import validate_rows


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


def validate_existing_csv(
    content: bytes,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> tuple[bool, str | None]:
    """Validate schema and every row of an existing canonical candidate."""

    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return False, "existing file is not valid UTF-8"

    try:
        records = list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        return False, f"existing file is not valid CSV: {exc}"

    if not records:
        return False, "existing file is empty"
    if tuple(records[0]) != tuple(columns):
        return False, "existing file has a non-canonical header"
    if len(records) == 1:
        return False, "existing file contains no data rows"

    parsed_rows = tuple(
        parse_field_values(record, row_index)
        for row_index, record in enumerate(records[1:], start=1)
    )
    validation = validate_rows(parsed_rows)
    if validation.rejected_rows:
        first = validation.rejected_rows[0]
        return (
            False,
            f"existing file row {first.row_index} is invalid: "
            + "; ".join(first.reasons),
        )
    return True, None


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
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
    except BaseException:
        try:
            temporary_path.unlink(missing_ok=True)
        finally:
            raise


def save_canonical_csv(
    rows: Iterable[ValidEquityRow],
    requested_date: date,
    output_dir: Path,
    columns: Sequence[str] = CANONICAL_COLUMNS,
) -> SaveResult:
    """Save new content, but never replace invalid or conflicting existing data."""

    materialized_rows = tuple(rows)
    if not materialized_rows:
        raise ValueError("refusing to save a canonical CSV without valid rows")
    content = canonical_csv_bytes(materialized_rows, columns)
    checksum = sha256_bytes(content)
    path = output_dir / f"market_{requested_date.isoformat()}.csv"

    if path.exists():
        existing_content = path.read_bytes()
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

    _atomic_write(path, content)
    return SaveResult(
        status=SaveStatus.CREATED,
        path=path,
        checksum=checksum,
    )
