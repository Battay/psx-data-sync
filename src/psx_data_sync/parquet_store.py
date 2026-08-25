"""Deterministic consolidated Parquet storage for verified PSX market data."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from . import __version__
from .exporter import canonical_csv_bytes, load_canonical_csv_rows
from .state import ValidEquityRow


PARQUET_SCHEMA_VERSION = "psx_market_parquet_schema_v2_consolidated"
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3

PARQUET_COLUMNS: tuple[str, ...] = (
    "market_date", "symbol", "ldcp", "open", "high", "low", "close",
    "change", "change_percent", "volume",
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
class ParquetSource:
    """Verified identity and location of one canonical CSV source."""

    market_date: date
    path: Path
    checksum: str
    row_count: int


@dataclass(frozen=True, slots=True)
class ParquetInspection:
    """Read-only integrity result for the consolidated Parquet artifact."""

    path: Path
    exists: bool
    valid: bool
    row_count: int = 0
    checksum: str | None = None
    source_identity: str | None = None
    source_date_count: int | None = None
    source_row_count: int | None = None
    schema_version: str | None = None
    file_size: int | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ParquetWriteResult:
    """Result of atomically publishing the consolidated dataset."""

    path: Path
    row_count: int
    checksum: str
    source_identity: str
    source_date_count: int


def consolidated_parquet_path(output_root: Path) -> Path:
    """Return the only canonical derived Parquet output path."""

    return Path(output_root) / "market.parquet"


def sha256_file(path: Path) -> str:
    """Return SHA-256 for the exact bytes of a file."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_source_manifest_identity(
    entries: Iterable[tuple[date, str, int]],
) -> str:
    """Hash an ordered date/checksum/row-count source manifest."""

    manifest = [
        {
            "market_date": market_date.isoformat(),
            "row_count": row_count,
            "sha256": checksum,
        }
        for market_date, checksum, row_count in sorted(entries, key=lambda item: item[0])
    ]
    encoded = json.dumps(
        manifest,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def compute_source_identity(sources: Iterable[ParquetSource]) -> str:
    """Hash the complete ordered manifest for validated source objects."""

    return compute_source_manifest_identity(
        (source.market_date, source.checksum, source.row_count) for source in sources
    )


def _metadata_text(metadata: dict[bytes, bytes] | None, key: bytes) -> str | None:
    if not metadata or key not in metadata:
        return None
    try:
        return metadata[key].decode("utf-8")
    except UnicodeDecodeError:
        return None


def _dataset_schema(sources: tuple[ParquetSource, ...]) -> pa.Schema:
    identity = compute_source_identity(sources)
    metadata = {
        b"psx_schema_version": PARQUET_SCHEMA_VERSION.encode("utf-8"),
        b"dataset_source_identity": identity.encode("ascii"),
        b"source_date_count": str(len(sources)).encode("ascii"),
        b"source_row_count": str(sum(source.row_count for source in sources)).encode(
            "ascii"
        ),
        b"source_identity_algorithm": b"sha256-canonical-json-v1",
        b"exporter_version": __version__.encode("utf-8"),
    }
    return PARQUET_SCHEMA.with_metadata(metadata)


def _rows_to_table(
    market_date: date,
    rows: tuple[ValidEquityRow, ...],
    schema: pa.Schema,
) -> pa.Table:
    ordered = sorted(rows, key=lambda row: row.symbol)
    symbols = [row.symbol for row in ordered]
    if len(symbols) != len(set(symbols)):
        raise ValueError(
            "duplicate (market_date, symbol) row for "
            f"{market_date.isoformat()}"
        )
    return pa.Table.from_arrays(
        [
            pa.array([market_date] * len(ordered), type=pa.date32()),
            pa.array(symbols, type=pa.string()),
            pa.array([float(row.ldcp) for row in ordered], type=pa.float64()),
            pa.array([float(row.open) for row in ordered], type=pa.float64()),
            pa.array([float(row.high) for row in ordered], type=pa.float64()),
            pa.array([float(row.low) for row in ordered], type=pa.float64()),
            pa.array([float(row.close) for row in ordered], type=pa.float64()),
            pa.array([float(row.change) for row in ordered], type=pa.float64()),
            pa.array([float(row.change_percent) for row in ordered], type=pa.float64()),
            pa.array([row.volume for row in ordered], type=pa.int64()),
        ],
        schema=schema,
    )


def _write_parquet_file(
    sources: tuple[ParquetSource, ...],
    destination: Path,
) -> None:
    schema = _dataset_schema(sources)
    with pq.ParquetWriter(
        destination,
        schema,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        use_dictionary=True,
        write_statistics=True,
    ) as writer:
        for source in sources:
            rows = load_canonical_csv_rows(source.path)
            snapshot = canonical_csv_bytes(rows)
            observed_checksum = hashlib.sha256(snapshot).hexdigest()
            if observed_checksum != source.checksum or len(rows) != source.row_count:
                raise ValueError(
                    f"canonical CSV changed before export: {source.path}"
                )
            try:
                current_bytes = source.path.read_bytes()
            except OSError as exc:
                raise ValueError(
                    f"canonical CSV cannot be reread: {source.path}: {exc}"
                ) from exc
            if current_bytes != snapshot:
                raise ValueError(
                    f"canonical CSV changed during export: {source.path}"
                )
            writer.write_table(_rows_to_table(source.market_date, rows, schema))


def inspect_parquet_file(path: Path) -> ParquetInspection:
    """Validate the consolidated file, schema, metadata, order, and keys."""

    path = Path(path)
    if not path.exists():
        return ParquetInspection(
            path=path, exists=False, valid=False, error="Parquet file does not exist"
        )

    checksum: str | None = None
    file_size: int | None = None
    try:
        checksum = sha256_file(path)
        file_size = path.stat().st_size
        table = pq.read_table(path)
    except Exception as exc:
        return ParquetInspection(
            path=path,
            exists=True,
            valid=False,
            checksum=checksum,
            file_size=file_size,
            error=f"Parquet file cannot be read: {exc}",
        )

    metadata = table.schema.metadata
    schema_version = _metadata_text(metadata, b"psx_schema_version")
    source_identity = _metadata_text(metadata, b"dataset_source_identity")
    source_date_count_text = _metadata_text(metadata, b"source_date_count")
    source_row_count_text = _metadata_text(metadata, b"source_row_count")
    identity_algorithm = _metadata_text(metadata, b"source_identity_algorithm")
    exporter_version = _metadata_text(metadata, b"exporter_version")
    try:
        source_date_count = int(source_date_count_text or "")
        source_row_count = int(source_row_count_text or "")
    except ValueError:
        source_date_count = None
        source_row_count = None

    common = dict(
        path=path,
        exists=True,
        row_count=table.num_rows,
        checksum=checksum,
        source_identity=source_identity,
        source_date_count=source_date_count,
        source_row_count=source_row_count,
        schema_version=schema_version,
        file_size=file_size,
    )

    def invalid(message: str) -> ParquetInspection:
        return ParquetInspection(valid=False, error=message, **common)

    if table.schema.remove_metadata() != PARQUET_SCHEMA:
        return invalid("Parquet schema does not match consolidated schema v2")
    if schema_version != PARQUET_SCHEMA_VERSION:
        return invalid("Parquet schema-version metadata mismatch")
    if not source_identity or len(source_identity) != 64:
        return invalid("missing or invalid dataset source identity metadata")
    try:
        int(source_identity, 16)
    except ValueError:
        return invalid("missing or invalid dataset source identity metadata")
    if source_date_count is None or source_date_count < 0:
        return invalid("missing or invalid source_date_count metadata")
    if source_row_count is None or source_row_count < 0:
        return invalid("missing or invalid source_row_count metadata")
    if source_row_count != table.num_rows:
        return invalid("Parquet row count disagrees with source metadata")
    if identity_algorithm != "sha256-canonical-json-v1":
        return invalid("source identity algorithm metadata mismatch")
    if not exporter_version:
        return invalid("missing exporter_version metadata")
    if any(table.column(name).null_count for name in PARQUET_COLUMNS):
        return invalid("Parquet contains null values in required columns")

    dates = table.column("market_date").to_pylist()
    symbols = table.column("symbol").to_pylist()
    keys = list(zip(dates, symbols, strict=True))
    if len(keys) != len(set(keys)):
        return invalid("Parquet contains duplicate (market_date, symbol) rows")
    if keys != sorted(keys):
        return invalid("Parquet rows are not sorted by market_date, symbol")
    if len(set(dates)) != source_date_count:
        return invalid("Parquet distinct date count disagrees with source metadata")
    return ParquetInspection(valid=True, error=None, **common)


def write_consolidated_parquet(
    sources: Iterable[ParquetSource], output_root: Path
) -> ParquetWriteResult:
    """Build, validate, and atomically replace the consolidated artifact."""

    source_tuple = tuple(sorted(sources, key=lambda item: item.market_date))
    if len({source.market_date for source in source_tuple}) != len(source_tuple):
        raise ValueError("duplicate market_date entries in source manifest")

    identity = compute_source_identity(source_tuple)
    destination = consolidated_parquet_path(output_root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".tmp",
    )
    os.close(descriptor)
    temporary_path = Path(temporary_name)

    try:
        _write_parquet_file(source_tuple, temporary_path)
        inspection = inspect_parquet_file(temporary_path)
        if not inspection.valid or inspection.source_identity != identity:
            raise ValueError(
                "generated Parquet failed validation: "
                f"{inspection.error or 'source identity mismatch'}"
            )
        os.replace(temporary_path, destination)
        final = inspect_parquet_file(destination)
        if not final.valid or final.checksum is None:
            raise ValueError(
                "published Parquet failed validation: "
                f"{final.error or 'unknown validation error'}"
            )
        if final.source_identity != identity:
            raise ValueError("published Parquet source identity mismatch")
        return ParquetWriteResult(
            path=destination,
            row_count=final.row_count,
            checksum=final.checksum,
            source_identity=identity,
            source_date_count=len(source_tuple),
        )
    finally:
        temporary_path.unlink(missing_ok=True)
