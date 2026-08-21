"""Orchestration service for derived Parquet partition synchronization."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

from .exporter import inspect_canonical_csv_file
from .parquet_store import (
    PARQUET_SCHEMA_VERSION,
    inspect_parquet_file,
    parquet_partition_path,
    write_parquet_partition,
)
from .state import (
    ParquetExportStatus,
    PersistentSyncStatus,
)
from .state_db import StateDatabaseError, StateRepository, utc_now_iso
from .synchronizer import generate_date_range

logger = logging.getLogger(__name__)


class ParquetExportAction(StrEnum):
    """Orchestration decision for one market date."""

    NO_ACTION = "NO_ACTION"
    CREATE = "CREATE"
    REBUILD_STALE = "REBUILD_STALE"
    REBUILD_CORRUPT = "REBUILD_CORRUPT"
    REBUILD_FORCED = "REBUILD_FORCED"
    REINDEX_CURRENT = "REINDEX_CURRENT"
    EXCLUDE_NON_TRADING = "EXCLUDE_NON_TRADING"
    EXCLUDE_UNRESOLVED = "EXCLUDE_UNRESOLVED"
    EXCLUDE_FAILURE = "EXCLUDE_FAILURE"
    EXCLUDE_FILE_ISSUE = "EXCLUDE_FILE_ISSUE"
    SOURCE_INVALID = "SOURCE_INVALID"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class DateParquetSyncResult:
    """Result of planning/synchronizing Parquet for one date."""

    market_date: str
    source_status: PersistentSyncStatus | None
    action: ParquetExportAction
    export_status_before: ParquetExportStatus | None
    export_status_planned: ParquetExportStatus | None
    export_status_after: ParquetExportStatus | None
    eligible: bool
    source_csv_path: Path | None
    source_checksum: str | None
    source_row_count: int | None
    parquet_path: Path | None
    parquet_checksum: str | None
    parquet_row_count: int | None
    dry_run: bool
    rebuilt_or_written: bool
    warnings: tuple[str, ...] = ()
    error: str | None = None

    @property
    def synchronized(self) -> bool:
        if self.action is ParquetExportAction.EXCLUDE_NON_TRADING:
            return True
        target_status = (
            self.export_status_planned
            if self.dry_run
            else self.export_status_after
        )
        return bool(
            self.error is None
            and target_status is ParquetExportStatus.CURRENT
            and self.action
            in {
                ParquetExportAction.NO_ACTION,
                ParquetExportAction.CREATE,
                ParquetExportAction.REBUILD_STALE,
                ParquetExportAction.REBUILD_CORRUPT,
                ParquetExportAction.REBUILD_FORCED,
                ParquetExportAction.REINDEX_CURRENT,
            }
        )


@dataclass(frozen=True, slots=True)
class RangeParquetSyncResult:
    """Result of planning/synchronizing Parquet for a date range."""

    start_date: str
    end_date: str
    requested_count: int
    eligible_count: int
    current_count: int
    create_count: int
    stale_count: int
    corrupt_count: int
    reindexed_count: int
    excluded_non_trading_count: int
    excluded_unresolved_count: int
    excluded_failure_count: int
    excluded_file_issue_count: int
    source_invalid_count: int
    failed_count: int
    written_or_rebuilt_count: int
    synchronized: bool
    synchronization_percentage: float
    dry_run: bool
    duration_ms: float
    results: tuple[DateParquetSyncResult, ...]
    warnings: tuple[str, ...] = ()


def sync_parquet_date(
    repository: StateRepository,
    market_date: str | date,
    *,
    output_root: Path | None = None,
    dry_run: bool = False,
    rebuild: bool = False,
) -> DateParquetSyncResult:
    """Plan or execute Parquet synchronization for one date."""

    date_text = (
        market_date.isoformat()
        if isinstance(market_date, date)
        else market_date
    )
    parsed_date = date.fromisoformat(date_text)
    root = (output_root or repository.project_root / "data" / "parquet").resolve()

    date_state = repository.get_date_state(date_text)
    db_record = repository.get_parquet_export(date_text)
    export_status_before = db_record.status if db_record else None

    # Step 1: Check source state & eligibility
    if date_state is None or date_state.status in (
        PersistentSyncStatus.NEVER_ATTEMPTED,
        PersistentSyncStatus.EMPTY_UNRESOLVED,
    ):
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status if date_state else None,
            action=ParquetExportAction.EXCLUDE_UNRESOLVED,
            export_status_before=export_status_before,
            export_status_planned=export_status_before,
            export_status_after=export_status_before,
            eligible=False,
            source_csv_path=None,
            source_checksum=date_state.csv_checksum_sha256 if date_state else None,
            source_row_count=date_state.valid_row_count if date_state else None,
            parquet_path=None,
            parquet_checksum=db_record.parquet_checksum_sha256 if db_record else None,
            parquet_row_count=db_record.parquet_row_count if db_record else None,
            dry_run=dry_run,
            rebuilt_or_written=False,
        )

    if date_state.status is PersistentSyncStatus.CONFIRMED_NON_TRADING:
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=ParquetExportAction.EXCLUDE_NON_TRADING,
            export_status_before=export_status_before,
            export_status_planned=export_status_before,
            export_status_after=export_status_before,
            eligible=False,
            source_csv_path=None,
            source_checksum=None,
            source_row_count=None,
            parquet_path=None,
            parquet_checksum=db_record.parquet_checksum_sha256 if db_record else None,
            parquet_row_count=db_record.parquet_row_count if db_record else None,
            dry_run=dry_run,
            rebuilt_or_written=False,
        )

    if date_state.status in (
        PersistentSyncStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.HTTP_FAILURE,
        PersistentSyncStatus.PARSE_FAILURE,
        PersistentSyncStatus.VALIDATION_FAILURE,
    ):
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=ParquetExportAction.EXCLUDE_FAILURE,
            export_status_before=export_status_before,
            export_status_planned=export_status_before,
            export_status_after=export_status_before,
            eligible=False,
            source_csv_path=None,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=None,
            parquet_checksum=db_record.parquet_checksum_sha256 if db_record else None,
            parquet_row_count=db_record.parquet_row_count if db_record else None,
            dry_run=dry_run,
            rebuilt_or_written=False,
        )

    if date_state.status in (
        PersistentSyncStatus.FILE_MISSING,
        PersistentSyncStatus.FILE_CORRUPT,
        PersistentSyncStatus.FILE_CONFLICT,
    ):
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=ParquetExportAction.EXCLUDE_FILE_ISSUE,
            export_status_before=export_status_before,
            export_status_planned=export_status_before,
            export_status_after=export_status_before,
            eligible=False,
            source_csv_path=None,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=None,
            parquet_checksum=db_record.parquet_checksum_sha256 if db_record else None,
            parquet_row_count=db_record.parquet_row_count if db_record else None,
            dry_run=dry_run,
            rebuilt_or_written=False,
        )

    # Date state status is VERIFIED_TRADING_DATA. Locate and inspect the CSV file.
    canonical_raw_path = (
        repository.raw_output_dir / f"market_{date_text}.csv"
    ).resolve()

    csv_path: Path | None = None
    if canonical_raw_path.exists():
        csv_path = canonical_raw_path
    elif date_state.csv_relative_path:
        rel_candidate = (
            repository.project_root / date_state.csv_relative_path
        ).resolve()
        if rel_candidate.exists():
            csv_path = rel_candidate

    if csv_path is None:
        csv_path = (
            (repository.project_root / date_state.csv_relative_path).resolve()
            if date_state.csv_relative_path
            else canonical_raw_path
        )

    source_invalid = False
    source_error = None
    if not csv_path.exists():
        source_invalid = True
        source_error = f"canonical CSV missing: {csv_path}"
    else:
        inspection = inspect_canonical_csv_file(csv_path)
        if not inspection.exists or not inspection.valid:
            source_invalid = True
            source_error = inspection.error or "canonical CSV is invalid"
        elif inspection.row_count <= 0:
            source_invalid = True
            source_error = "canonical CSV contains 0 rows"
        else:
            # Source CSV is valid on disk. Check database metadata match.
            if (
                date_state.csv_checksum_sha256 != inspection.checksum
                or date_state.valid_row_count != inspection.row_count
            ):
                if not dry_run:
                    repository.index_local_file(csv_path)
                    refreshed_state = repository.get_date_state(date_text)
                    if refreshed_state:
                        date_state = refreshed_state

            expected_checksum = date_state.csv_checksum_sha256 or (
                inspection.checksum if inspection.valid else None
            )
            expected_row_count = date_state.valid_row_count or (
                inspection.row_count if inspection.valid else None
            )

            if inspection.checksum != expected_checksum:
                source_invalid = True
                source_error = (
                    f"checksum mismatch: CSV {inspection.checksum} != "
                    f"DB {expected_checksum}"
                )
            elif inspection.row_count != expected_row_count:
                source_invalid = True
                source_error = (
                    f"row count mismatch: CSV {inspection.row_count} != "
                    f"DB {expected_row_count}"
                )

    if source_invalid:
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=ParquetExportAction.SOURCE_INVALID,
            export_status_before=export_status_before,
            export_status_planned=export_status_before,
            export_status_after=export_status_before,
            eligible=False,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=None,
            parquet_checksum=db_record.parquet_checksum_sha256 if db_record else None,
            parquet_row_count=db_record.parquet_row_count if db_record else None,
            dry_run=dry_run,
            rebuilt_or_written=False,
            error=source_error,
        )

    # Source CSV is valid & eligible!
    expected_parquet_path = parquet_partition_path(root, parsed_date)
    parquet_inspection = inspect_parquet_file(
        expected_parquet_path,
        expected_market_date=parsed_date,
    )

    # Classification logic
    if not parquet_inspection.exists:
        action = ParquetExportAction.CREATE
        planned_status = ParquetExportStatus.CURRENT
    elif parquet_inspection.valid:
        if (
            parquet_inspection.schema_version == PARQUET_SCHEMA_VERSION
            and parquet_inspection.source_csv_checksum == date_state.csv_checksum_sha256
            and parquet_inspection.source_row_count == date_state.valid_row_count
            and parquet_inspection.row_count == date_state.valid_row_count
        ):
            if rebuild:
                action = ParquetExportAction.REBUILD_FORCED
                planned_status = ParquetExportStatus.CURRENT
            elif (
                db_record is not None
                and db_record.status is ParquetExportStatus.CURRENT
                and db_record.source_csv_checksum_sha256 == date_state.csv_checksum_sha256
                and db_record.source_row_count == date_state.valid_row_count
                and db_record.parquet_checksum_sha256 == parquet_inspection.checksum
            ):
                action = ParquetExportAction.NO_ACTION
                planned_status = ParquetExportStatus.CURRENT
            else:
                action = ParquetExportAction.REINDEX_CURRENT
                planned_status = ParquetExportStatus.CURRENT
        else:
            action = ParquetExportAction.REBUILD_STALE
            planned_status = ParquetExportStatus.CURRENT
    else:
        # Invalid / corrupt file on disk
        if parquet_inspection.error in (
            "Parquet source checksum is stale",
            "Parquet row count does not match verified source",
            "Parquet schema-version metadata mismatch",
            "Parquet schema does not match D5 schema v1",
        ):
            action = ParquetExportAction.REBUILD_STALE
            planned_status = ParquetExportStatus.CURRENT
        else:
            action = ParquetExportAction.REBUILD_CORRUPT
            planned_status = ParquetExportStatus.CURRENT

    if dry_run:
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=action,
            export_status_before=export_status_before,
            export_status_planned=planned_status,
            export_status_after=export_status_before,
            eligible=True,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=expected_parquet_path,
            parquet_checksum=parquet_inspection.checksum,
            parquet_row_count=parquet_inspection.row_count,
            dry_run=True,
            rebuilt_or_written=False,
        )

    # Apply mode execution
    if action is ParquetExportAction.NO_ACTION:
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=action,
            export_status_before=export_status_before,
            export_status_planned=planned_status,
            export_status_after=ParquetExportStatus.CURRENT,
            eligible=True,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=expected_parquet_path,
            parquet_checksum=parquet_inspection.checksum,
            parquet_row_count=parquet_inspection.row_count,
            dry_run=False,
            rebuilt_or_written=False,
        )

    if action is ParquetExportAction.REINDEX_CURRENT:
        now = utc_now_iso()
        record = repository.upsert_parquet_export(
            date_text,
            status=ParquetExportStatus.CURRENT,
            schema_version=PARQUET_SCHEMA_VERSION,
            source_csv_checksum_sha256=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=expected_parquet_path,
            parquet_checksum_sha256=parquet_inspection.checksum,
            parquet_row_count=parquet_inspection.row_count,
            verified_at=now,
        )
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=action,
            export_status_before=export_status_before,
            export_status_planned=planned_status,
            export_status_after=record.status,
            eligible=True,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=expected_parquet_path,
            parquet_checksum=record.parquet_checksum_sha256,
            parquet_row_count=record.parquet_row_count,
            dry_run=False,
            rebuilt_or_written=False,
        )

    # Action is CREATE, REBUILD_STALE, REBUILD_CORRUPT, or REBUILD_FORCED
    try:
        assert csv_path is not None
        write_result = write_parquet_partition(parsed_date, csv_path, root)
        now = utc_now_iso()
        record = repository.upsert_parquet_export(
            date_text,
            status=ParquetExportStatus.CURRENT,
            schema_version=PARQUET_SCHEMA_VERSION,
            source_csv_checksum_sha256=write_result.source_csv_checksum,
            source_row_count=date_state.valid_row_count,
            parquet_path=write_result.path,
            parquet_checksum_sha256=write_result.checksum,
            parquet_row_count=write_result.row_count,
            verified_at=now,
        )
        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=action,
            export_status_before=export_status_before,
            export_status_planned=planned_status,
            export_status_after=record.status,
            eligible=True,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=write_result.path,
            parquet_checksum=write_result.checksum,
            parquet_row_count=write_result.row_count,
            dry_run=False,
            rebuilt_or_written=True,
        )
    except Exception as exc:
        logger.exception(f"failed to export Parquet partition for {date_text}")
        err_msg = str(exc)
        after_status: ParquetExportStatus = ParquetExportStatus.FAILED
        try:
            record = repository.upsert_parquet_export(
                date_text,
                status=ParquetExportStatus.FAILED,
                schema_version=PARQUET_SCHEMA_VERSION,
                source_csv_checksum_sha256=date_state.csv_checksum_sha256,
                source_row_count=date_state.valid_row_count,
                parquet_path=expected_parquet_path if expected_parquet_path.exists() else None,
                parquet_checksum_sha256=parquet_inspection.checksum if parquet_inspection.exists else None,
                parquet_row_count=parquet_inspection.row_count if parquet_inspection.exists else None,
                last_error=err_msg,
            )
            after_status = record.status
        except Exception:
            pass

        return DateParquetSyncResult(
            market_date=date_text,
            source_status=date_state.status,
            action=ParquetExportAction.FAILED,
            export_status_before=export_status_before,
            export_status_planned=planned_status,
            export_status_after=after_status,
            eligible=True,
            source_csv_path=csv_path,
            source_checksum=date_state.csv_checksum_sha256,
            source_row_count=date_state.valid_row_count,
            parquet_path=expected_parquet_path if expected_parquet_path.exists() else None,
            parquet_checksum=parquet_inspection.checksum,
            parquet_row_count=parquet_inspection.row_count,
            dry_run=False,
            rebuilt_or_written=False,
            error=err_msg,
        )


def sync_parquet_range(
    repository: StateRepository,
    start_date: str | date,
    end_date: str | date,
    *,
    output_root: Path | None = None,
    dry_run: bool = False,
    rebuild: bool = False,
) -> RangeParquetSyncResult:
    """Plan or execute Parquet synchronization for an inclusive date range."""

    start_text = (
        start_date.isoformat()
        if isinstance(start_date, date)
        else start_date
    )
    end_text = (
        end_date.isoformat()
        if isinstance(end_date, date)
        else end_date
    )

    dates = generate_date_range(start_text, end_text)
    started = time.perf_counter()

    results: list[DateParquetSyncResult] = []
    for day in dates:
        try:
            result = sync_parquet_date(
                repository,
                day,
                output_root=output_root,
                dry_run=dry_run,
                rebuild=rebuild,
            )
        except Exception as exc:
            logger.exception(f"unhandled exception syncing Parquet for {day}")
            result = DateParquetSyncResult(
                market_date=day,
                source_status=None,
                action=ParquetExportAction.FAILED,
                export_status_before=None,
                export_status_planned=None,
                export_status_after=ParquetExportStatus.FAILED,
                eligible=False,
                source_csv_path=None,
                source_checksum=None,
                source_row_count=None,
                parquet_path=None,
                parquet_checksum=None,
                parquet_row_count=None,
                dry_run=dry_run,
                rebuilt_or_written=False,
                error=str(exc),
            )
        results.append(result)

    duration_ms = (time.perf_counter() - started) * 1000.0

    eligible_count = sum(1 for r in results if r.eligible)
    current_count = sum(
        1 for r in results if r.action is ParquetExportAction.NO_ACTION
    )
    create_count = sum(
        1 for r in results if r.action is ParquetExportAction.CREATE
    )
    stale_count = sum(
        1 for r in results if r.action is ParquetExportAction.REBUILD_STALE
    )
    corrupt_count = sum(
        1 for r in results if r.action is ParquetExportAction.REBUILD_CORRUPT
    )
    reindexed_count = sum(
        1 for r in results if r.action is ParquetExportAction.REINDEX_CURRENT
    )
    excluded_non_trading_count = sum(
        1 for r in results if r.action is ParquetExportAction.EXCLUDE_NON_TRADING
    )
    excluded_unresolved_count = sum(
        1 for r in results if r.action is ParquetExportAction.EXCLUDE_UNRESOLVED
    )
    excluded_failure_count = sum(
        1 for r in results if r.action is ParquetExportAction.EXCLUDE_FAILURE
    )
    excluded_file_issue_count = sum(
        1 for r in results if r.action is ParquetExportAction.EXCLUDE_FILE_ISSUE
    )
    source_invalid_count = sum(
        1 for r in results if r.action is ParquetExportAction.SOURCE_INVALID
    )
    failed_count = sum(
        1 for r in results if r.action is ParquetExportAction.FAILED or r.error is not None
    )
    written_or_rebuilt_count = sum(1 for r in results if r.rebuilt_or_written)

    synchronized_count = sum(1 for r in results if r.synchronized)
    total_requested = len(results)
    synchronization_percentage = (
        (synchronized_count / total_requested) * 100.0
        if total_requested > 0
        else 100.0
    )
    is_synchronized = synchronized_count == total_requested

    return RangeParquetSyncResult(
        start_date=start_text,
        end_date=end_text,
        requested_count=total_requested,
        eligible_count=eligible_count,
        current_count=current_count,
        create_count=create_count,
        stale_count=stale_count,
        corrupt_count=corrupt_count,
        reindexed_count=reindexed_count,
        excluded_non_trading_count=excluded_non_trading_count,
        excluded_unresolved_count=excluded_unresolved_count,
        excluded_failure_count=excluded_failure_count,
        excluded_file_issue_count=excluded_file_issue_count,
        source_invalid_count=source_invalid_count,
        failed_count=failed_count,
        written_or_rebuilt_count=written_or_rebuilt_count,
        synchronized=is_synchronized,
        synchronization_percentage=synchronization_percentage,
        dry_run=dry_run,
        duration_ms=duration_ms,
        results=tuple(results),
    )
