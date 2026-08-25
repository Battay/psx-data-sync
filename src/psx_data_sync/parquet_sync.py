"""Synchronization service for the consolidated derived Parquet dataset."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import StrEnum
from pathlib import Path

from .exporter import canonical_csv_bytes, load_canonical_csv_rows
from .parquet_store import (
    ParquetSource,
    compute_source_manifest_identity,
    consolidated_parquet_path,
    inspect_parquet_file,
    write_consolidated_parquet,
)
from .state import ParquetExportStatus, PersistentSyncStatus
from .state_db import StateRepository

logger = logging.getLogger(__name__)


class ParquetExportAction(StrEnum):
    """Consolidated dataset synchronization decision."""

    NO_ACTION = "NO_ACTION"
    CREATE = "CREATE"
    REBUILD_STALE = "REBUILD_STALE"
    REBUILD_CORRUPT = "REBUILD_CORRUPT"
    REBUILD_FORCED = "REBUILD_FORCED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class ConsolidatedParquetSyncResult:
    """Plan or apply result for the complete verified local source set."""

    source_dates: int
    source_rows: int
    status: ParquetExportStatus
    planned_status: ParquetExportStatus
    action: ParquetExportAction
    rows_written: int
    output_path: Path
    source_identity: str | None
    legacy_partition_count: int
    file_size: int | None
    last_build: str | None
    dry_run: bool
    rebuild: bool
    duration_ms: float
    errors: tuple[str, ...] = ()

    @property
    def synchronized(self) -> bool:
        return self.status is ParquetExportStatus.CURRENT and not self.errors


def _legacy_partition_count(output_root: Path) -> int:
    legacy_root = Path(output_root) / "market"
    if not legacy_root.exists():
        return 0
    return sum(1 for path in legacy_root.rglob("*.parquet") if path.is_file())


def _last_build(path: Path) -> str | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _source_path(
    repository: StateRepository, market_date: str, relative: str | None
) -> Path:
    canonical = (repository.raw_output_dir / f"market_{market_date}.csv").resolve()
    if canonical.exists():
        return canonical
    if relative:
        candidate = (repository.project_root / relative).resolve()
        if candidate.exists():
            return candidate
    return canonical


def _discover_sources(
    repository: StateRepository,
) -> tuple[tuple[ParquetSource, ...], tuple[str, ...], int, int, str]:
    states = repository.list_dates_by_status(
        (
            PersistentSyncStatus.VERIFIED_TRADING_DATA,
            PersistentSyncStatus.ALREADY_PRESENT_VERIFIED,
        )
    )
    sources: list[ParquetSource] = []
    errors: list[str] = []
    manifest = tuple(
        (
            date.fromisoformat(state.market_date),
            state.csv_checksum_sha256 or "",
            state.valid_row_count,
        )
        for state in states
    )

    for state in states:
        path = _source_path(repository, state.market_date, state.csv_relative_path)
        prefix = f"{state.market_date}: "
        if not path.exists():
            errors.append(prefix + f"canonical CSV is missing: {path}")
            continue
        try:
            rows = load_canonical_csv_rows(path)
        except Exception as exc:
            errors.append(prefix + f"canonical CSV validation failed: {exc}")
            continue
        snapshot = canonical_csv_bytes(rows)
        checksum = hashlib.sha256(snapshot).hexdigest()
        try:
            if path.read_bytes() != snapshot:
                errors.append(prefix + "canonical CSV changed during source discovery")
                continue
        except OSError as exc:
            errors.append(prefix + f"canonical CSV cannot be reread: {exc}")
            continue
        if not state.csv_checksum_sha256:
            errors.append(prefix + "verified state has no canonical CSV checksum")
            continue
        if checksum != state.csv_checksum_sha256:
            errors.append(
                prefix
                + "canonical CSV checksum differs from verified state "
                + f"({checksum} != {state.csv_checksum_sha256})"
            )
            continue
        if len(rows) != state.valid_row_count:
            errors.append(
                prefix
                + "canonical CSV row count differs from verified state "
                + f"({len(rows)} != {state.valid_row_count})"
            )
            continue
        sources.append(
            ParquetSource(
                market_date=date.fromisoformat(state.market_date),
                path=path,
                checksum=checksum,
                row_count=len(rows),
            )
        )

    return (
        tuple(sources),
        tuple(errors),
        len(states),
        sum(state.valid_row_count for state in states),
        compute_source_manifest_identity(manifest),
    )


def sync_consolidated_parquet(
    repository: StateRepository,
    *,
    output_root: Path | None = None,
    dry_run: bool = True,
    rebuild: bool = False,
) -> ConsolidatedParquetSyncResult:
    """Plan or build one dataset from every verified canonical local CSV."""

    started = time.perf_counter()
    root = (output_root or repository.project_root / "data" / "parquet").resolve()
    output_path = consolidated_parquet_path(root)
    legacy_count = _legacy_partition_count(root)
    sources, source_errors, source_dates, source_rows, identity = _discover_sources(
        repository
    )
    inspection = inspect_parquet_file(output_path)

    if not inspection.exists:
        status = ParquetExportStatus.MISSING
        action = ParquetExportAction.CREATE
    elif not inspection.valid:
        status = ParquetExportStatus.CORRUPT
        action = ParquetExportAction.REBUILD_CORRUPT
    elif (
        inspection.source_identity != identity
        or inspection.source_date_count != source_dates
        or inspection.source_row_count != source_rows
        or inspection.row_count != source_rows
    ):
        status = ParquetExportStatus.STALE
        action = ParquetExportAction.REBUILD_STALE
    elif rebuild:
        status = ParquetExportStatus.CURRENT
        action = ParquetExportAction.REBUILD_FORCED
    else:
        status = ParquetExportStatus.CURRENT
        action = ParquetExportAction.NO_ACTION

    if source_errors:
        failure_status = ParquetExportStatus.FAILED if not dry_run else status
        if dry_run and status is ParquetExportStatus.CURRENT:
            failure_status = ParquetExportStatus.STALE
        return ConsolidatedParquetSyncResult(
            source_dates=source_dates,
            source_rows=source_rows,
            status=failure_status,
            planned_status=ParquetExportStatus.FAILED,
            action=ParquetExportAction.FAILED,
            rows_written=0,
            output_path=output_path,
            source_identity=identity,
            legacy_partition_count=legacy_count,
            file_size=inspection.file_size,
            last_build=_last_build(output_path),
            dry_run=dry_run,
            rebuild=rebuild,
            duration_ms=(time.perf_counter() - started) * 1000.0,
            errors=source_errors,
        )

    if dry_run or action is ParquetExportAction.NO_ACTION:
        return ConsolidatedParquetSyncResult(
            source_dates=source_dates,
            source_rows=source_rows,
            status=status,
            planned_status=ParquetExportStatus.CURRENT,
            action=action,
            rows_written=0,
            output_path=output_path,
            source_identity=identity,
            legacy_partition_count=legacy_count,
            file_size=inspection.file_size,
            last_build=_last_build(output_path),
            dry_run=dry_run,
            rebuild=rebuild,
            duration_ms=(time.perf_counter() - started) * 1000.0,
        )

    try:
        write_result = write_consolidated_parquet(sources, root)
        final = inspect_parquet_file(write_result.path)
        if (
            not final.valid
            or final.source_identity != identity
            or final.source_date_count != source_dates
            or final.row_count != source_rows
        ):
            raise ValueError(final.error or "published dataset identity mismatch")
        return ConsolidatedParquetSyncResult(
            source_dates=source_dates,
            source_rows=source_rows,
            status=ParquetExportStatus.CURRENT,
            planned_status=ParquetExportStatus.CURRENT,
            action=action,
            rows_written=write_result.row_count,
            output_path=write_result.path,
            source_identity=identity,
            legacy_partition_count=legacy_count,
            file_size=final.file_size,
            last_build=_last_build(write_result.path),
            dry_run=False,
            rebuild=rebuild,
            duration_ms=(time.perf_counter() - started) * 1000.0,
        )
    except Exception as exc:
        logger.exception("failed to build consolidated Parquet dataset")
        current = inspect_parquet_file(output_path)
        return ConsolidatedParquetSyncResult(
            source_dates=source_dates,
            source_rows=source_rows,
            status=ParquetExportStatus.FAILED,
            planned_status=ParquetExportStatus.FAILED,
            action=ParquetExportAction.FAILED,
            rows_written=0,
            output_path=output_path,
            source_identity=identity,
            legacy_partition_count=legacy_count,
            file_size=current.file_size,
            last_build=_last_build(output_path),
            dry_run=False,
            rebuild=rebuild,
            duration_ms=(time.perf_counter() - started) * 1000.0,
            errors=(str(exc),),
        )
