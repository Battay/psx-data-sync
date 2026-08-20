"""Deterministic derived Parquet storage for verified PSX market data."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import __version__
from .exporter import load_canonical_csv_rows
from .state import ValidEquityRow


PARQUET_SCHEMA_VERSION = "psx_market_parquet_schema_v1"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3

PARQUET_COLUMNS: tuple[str, ...] = (
    "market_date",
    "symbol",
    "ldcp",
    "open",
    "high",
    "low",
    "close",
    "change",
    "change_percent",
    "volume",
)

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("market_date", pa.date32(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("ldcp", pa.float64(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("change", pa.float64(), nullable=False),
        pa.field("change_percent", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
    ]
)


@dataclass(frozen=True, slots=True)
class ParquetInspection:
    """Read-only integrity result for one market-date Parquet partition."""

    path: Path
    exists: bool
    valid: bool
    market_date: date | None = None
    row_count: int = 0
    checksum: str | None = None
    source_csv_checksum: str | None = None
    source_row_count: int | None = None
    schema_version: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ParquetWriteResult:
    """Result of atomically publishing one derived market partition."""

    market_date: date
    path: Path
    row_count: int
    checksum: str
    source_csv_checksum: str


def parquet_partition_path(output_root: Path, market_date: date) -> Path:
    """Return the deterministic final path for one market date."""

    return (
        Path(output_root)
        / "market"
        / f"market_date={market_date.isoformat()}"
        / "part-0.parquet"
    )


def sha256_file(path: Path) -> str:
    """Return SHA-256 for the exact bytes of a file."""

    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _rows_to_table(
    market_date: date,
    rows: tuple[ValidEquityRow, ...],
    *,
    source_csv_checksum: str,
) -> pa.Table:
    """Convert validated rows to a stable Arrow table."""

    ordered_rows = sorted(
        rows,
        key=lambda row: (row.symbol.casefold(), row.symbol),
    )

    metadata = {
        b"psx_schema_version": PARQUET_SCHEMA_VERSION.encode("utf-8"),
        b"source_csv_sha256": source_csv_checksum.encode("ascii"),
        b"source_valid_row_count": str(len(ordered_rows)).encode("ascii"),
        b"market_date": market_date.isoformat().encode("ascii"),
        b"exporter_version": __version__.encode("utf-8"),
    }

    schema = PARQUET_SCHEMA.with_metadata(metadata)

    return pa.Table.from_arrays(
        [
            pa.array(
                [market_date] * len(ordered_rows),
                type=pa.date32(),
            ),
            pa.array(
                [row.symbol for row in ordered_rows],
                type=pa.string(),
            ),
            pa.array(
                [float(row.ldcp) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.open) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.high) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.low) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.close) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.change) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [float(row.change_percent) for row in ordered_rows],
                type=pa.float64(),
            ),
            pa.array(
                [row.volume for row in ordered_rows],
                type=pa.int64(),
            ),
        ],
        schema=schema,
    )


def _metadata_text(
    metadata: dict[bytes, bytes] | None,
    key: bytes,
) -> str | None:
    if not metadata:
        return None

    value = metadata.get(key)
    if value is None:
        return None

    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return None


def inspect_parquet_file(
    path: Path,
    *,
    expected_market_date: date | None = None,
    expected_source_checksum: str | None = None,
    expected_source_row_count: int | None = None,
) -> ParquetInspection:
    """Validate one derived Parquet file without modifying it."""

    path = Path(path)

    if not path.exists():
        return ParquetInspection(
            path=path,
            exists=False,
            valid=False,
            error="Parquet file does not exist",
        )

    try:
        table = pq.read_table(path)
    except Exception as exc:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            checksum=sha256_file(path),
            error=f"Parquet file cannot be read: {exc}",
        )

    checksum = sha256_file(path)

    schema_metadata = table.schema.metadata
    schema_version = _metadata_text(
        schema_metadata,
        b"psx_schema_version",
    )
    source_checksum = _metadata_text(
        schema_metadata,
        b"source_csv_sha256",
    )
    source_row_count_text = _metadata_text(
        schema_metadata,
        b"source_valid_row_count",
    )
    metadata_market_date = _metadata_text(
        schema_metadata,
        b"market_date",
    )

    try:
        source_row_count = (
            int(source_row_count_text)
            if source_row_count_text is not None
            else None
        )
    except ValueError:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            checksum=checksum,
            schema_version=schema_version,
            source_csv_checksum=source_checksum,
            error="invalid source_valid_row_count metadata",
        )

    if table.schema.remove_metadata() != PARQUET_SCHEMA:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet schema does not match D5 schema v1",
        )

    if schema_version != PARQUET_SCHEMA_VERSION:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet schema-version metadata mismatch",
        )

    try:
        stored_market_date = (
            date.fromisoformat(metadata_market_date)
            if metadata_market_date is not None
            else None
        )
    except ValueError:
        stored_market_date = None

    if stored_market_date is None:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="missing or invalid market_date metadata",
        )

    date_values = table.column("market_date").to_pylist()
    if any(value != stored_market_date for value in date_values):
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet rows contain an unexpected market_date",
        )

    symbols = table.column("symbol").to_pylist()
    if len(symbols) != len(set(symbols)):
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet partition contains duplicate symbols",
        )

    if source_row_count != table.num_rows:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet row count disagrees with source metadata",
        )

    if (
        expected_market_date is not None
        and stored_market_date != expected_market_date
    ):
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet market date does not match expected date",
        )

    if (
        expected_source_checksum is not None
        and source_checksum != expected_source_checksum
    ):
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet source checksum is stale",
        )

    if (
        expected_source_row_count is not None
        and table.num_rows != expected_source_row_count
    ):
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            market_date=stored_market_date,
            row_count=table.num_rows,
            checksum=checksum,
            source_csv_checksum=source_checksum,
            source_row_count=source_row_count,
            schema_version=schema_version,
            error="Parquet row count does not match verified source",
        )

    return ParquetInspection(
        path=path,
        exists=True,
        valid=True,
        market_date=stored_market_date,
        row_count=table.num_rows,
        checksum=checksum,
        source_csv_checksum=source_checksum,
        source_row_count=source_row_count,
        schema_version=schema_version,
    )


def write_parquet_partition(
    market_date: date,
    source_csv_path: Path,
    output_root: Path,
) -> ParquetWriteResult:
    """Build, validate, and atomically publish one Parquet partition."""

    source_csv_path = Path(source_csv_path)

    source_content = source_csv_path.read_bytes()
    source_checksum = hashlib.sha256(source_content).hexdigest()

    rows = load_canonical_csv_rows(source_csv_path)

    table = _rows_to_table(
        market_date,
        rows,
        source_csv_checksum=source_checksum,
    )

    destination = parquet_partition_path(
        output_root,
        market_date,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)

    temporary_path = Path(temporary_name)

    try:
        pq.write_table(
            table,
            temporary_path,
            compression=PARQUET_COMPRESSION,
            compression_level=PARQUET_COMPRESSION_LEVEL,
            use_dictionary=True,
            write_statistics=True,
        )

        inspection = inspect_parquet_file(
            temporary_path,
            expected_market_date=market_date,
            expected_source_checksum=source_checksum,
            expected_source_row_count=len(rows),
        )

        if not inspection.valid:
            raise ValueError(
                "generated Parquet failed validation: "
                f"{inspection.error or 'unknown validation error'}"
            )

        os.replace(temporary_path, destination)

        final_inspection = inspect_parquet_file(
            destination,
            expected_market_date=market_date,
            expected_source_checksum=source_checksum,
            expected_source_row_count=len(rows),
        )

        if not final_inspection.valid or final_inspection.checksum is None:
            raise ValueError(
                "published Parquet failed validation: "
                f"{final_inspection.error or 'unknown validation error'}"
            )

        return ParquetWriteResult(
            market_date=market_date,
            path=destination,
            row_count=final_inspection.row_count,
            checksum=final_inspection.checksum,
            source_csv_checksum=source_checksum,
        )

    finally:
        temporary_path.unlink(missing_ok=True)
