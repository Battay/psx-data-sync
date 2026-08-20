from __future__ import annotations

import hashlib
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.parquet_store import inspect_parquet_file, parquet_partition_path
from psx_data_sync.parquet_sync import (
    DateParquetSyncResult,
    ParquetExportAction,
    RangeParquetSyncResult,
    sync_parquet_date,
    sync_parquet_range,
)
from psx_data_sync.state import (
    DownloadAttemptEvent,
    DownloadStatus,
    ParquetExportStatus,
    PersistentSyncStatus,
    ValidEquityRow,
)
from psx_data_sync.state_db import StateRepository


def make_repository(database_path: Path, project_root: Path) -> StateRepository:
    repo = StateRepository(
        database_path,
        project_root=project_root,
        source_endpoint="https://dps.psx.com.pk/historical",
    )
    repo.initialize()
    return repo


def _row(symbol: str, row_index: int = 1) -> ValidEquityRow:
    return ValidEquityRow(
        row_index=row_index,
        symbol=symbol,
        ldcp=Decimal("100.10"),
        open=Decimal("101.20"),
        high=Decimal("105.30"),
        low=Decimal("99.40"),
        close=Decimal("104.50"),
        change=Decimal("4.40"),
        change_percent=Decimal("4.3956"),
        volume=123456,
    )


def seed_verified_date(
    repo: StateRepository,
    market_date: str = "2026-08-07",
    rows: tuple[ValidEquityRow, ...] = (_row("AAA", 1), _row("BBB", 2)),
) -> Path:
    raw_dir = repo.project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / f"market_{market_date}.csv"

    content = canonical_csv_bytes(rows)
    csv_path.write_bytes(content)
    checksum = hashlib.sha256(content).hexdigest()

    run_id = repo.begin_sync_run("fetch", market_date, market_date, 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date=market_date,
            attempt_number=1,
            started_at="2026-08-07T10:00:00+00:00",
            finished_at="2026-08-07T10:00:01+00:00",
            duration_ms=1000.0,
            http_status=200,
            response_bytes=len(content),
            response_classification="EQUITY_ROWS",
            final_status=DownloadStatus.TRADING_DATA,
            retryable=False,
            parsed_row_count=len(rows),
            valid_row_count=len(rows),
            checksum=checksum,
            saved_path=csv_path,
        ),
    )
    return csv_path


# --- Eligibility Tests ---


def test_verified_healthy_csv_eligible(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is True
    assert result.action is ParquetExportAction.CREATE


def test_confirmed_non_trading_excluded_resolved(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    run_id = repo.begin_sync_run("fetch", "2026-08-09", "2026-08-09", 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date="2026-08-09",
            attempt_number=1,
            started_at="2026-08-09T10:00:00+00:00",
            finished_at="2026-08-09T10:00:01+00:00",
            duration_ms=500.0,
            http_status=200,
            response_bytes=100,
            response_classification="EMPTY_MARKET_RESPONSE",
            final_status=DownloadStatus.CONFIRMED_NON_TRADING,
            retryable=False,
        ),
    )

    result = sync_parquet_date(repo, "2026-08-09", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.EXCLUDE_NON_TRADING
    assert result.synchronized is True


def test_unresolved_excluded_incomplete(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)

    assert result.eligible is False
    assert result.action is ParquetExportAction.EXCLUDE_UNRESOLVED
    assert result.synchronized is False


def test_failure_excluded_incomplete(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    run_id = repo.begin_sync_run("fetch", "2026-08-07", "2026-08-07", 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date="2026-08-07",
            attempt_number=1,
            started_at="2026-08-07T10:00:00+00:00",
            finished_at="2026-08-07T10:00:01+00:00",
            duration_ms=1000.0,
            http_status=500,
            response_bytes=0,
            response_classification=None,
            final_status=DownloadStatus.HTTP_FAILURE,
            retryable=True,
            error_type="HTTP",
            error_message="Server error",
        ),
    )

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.EXCLUDE_FAILURE
    assert result.synchronized is False


def test_file_issue_excluded_incomplete(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    with sqlite3.connect(repo.database_path) as conn:
        conn.execute(
            """
            INSERT INTO date_sync_state (
                market_date, status, evidence_state, source_endpoint,
                record_created_at, record_updated_at
            ) VALUES ('2026-08-07', 'FILE_CORRUPT', 'NONE', 'url', 'now', 'now')
            """
        )

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.EXCLUDE_FILE_ISSUE
    assert result.synchronized is False


def test_verified_state_missing_csv_source_invalid(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    # Delete the CSV file
    csv_path = tmp_path / "data" / "raw" / "market_2026-08-07.csv"
    csv_path.unlink()

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.SOURCE_INVALID
    assert "missing" in (result.error or "")

    # Ensure D4 date state was NOT mutated
    state = repo.get_date_state("2026-08-07")
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA


def test_verified_state_checksum_mismatch_source_invalid(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo, "2026-08-07")

    # Tamper with the CSV file content
    csv_path.write_bytes(b"tampered content")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.SOURCE_INVALID


def test_verified_state_row_count_mismatch_source_invalid(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(
        repo, "2026-08-07", rows=(_row("AAA", 1), _row("BBB", 2))
    )

    # Overwrite CSV with only 1 row
    csv_path.write_bytes(canonical_csv_bytes((_row("AAA", 1),)))

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.eligible is False
    assert result.action is ParquetExportAction.SOURCE_INVALID


# --- Planning Tests ---


def test_missing_parquet_create(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.action is ParquetExportAction.CREATE
    assert result.export_status_planned is ParquetExportStatus.CURRENT


def test_valid_current_parquet_no_action(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    # First apply creates Parquet and DB record
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    # Second check plans NO_ACTION
    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.action is ParquetExportAction.NO_ACTION


def test_current_file_missing_db_record_reindex_current(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    # Build file in apply mode
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    # Delete DB record
    with sqlite3.connect(repo.database_path) as conn:
        conn.execute("DELETE FROM parquet_exports")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.action is ParquetExportAction.REINDEX_CURRENT


def test_stale_source_checksum_rebuild_stale(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    # Simulate updated verified source CSV (e.g. after D4 repair/re-verification)
    new_rows = (_row("AAA", 1), _row("BBB", 2), _row("CCC", 3))
    new_content = canonical_csv_bytes(new_rows)
    csv_path.write_bytes(new_content)
    new_checksum = hashlib.sha256(new_content).hexdigest()

    with sqlite3.connect(repo.database_path) as conn:
        conn.execute(
            """
            UPDATE date_sync_state
            SET csv_checksum_sha256 = ?, valid_row_count = 3
            WHERE market_date = '2026-08-07'
            """,
            (new_checksum,),
        )

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.action is ParquetExportAction.REBUILD_STALE


def test_corrupt_parquet_rebuild_corrupt(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    parquet_path = parquet_partition_path(
        tmp_path / "data" / "parquet", date(2026, 8, 7)
    )
    parquet_path.parent.mkdir(parents=True)
    parquet_path.write_bytes(b"invalid garbage header")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)
    assert result.action is ParquetExportAction.REBUILD_CORRUPT


# --- Dry Run Tests ---


def test_dry_run_performs_no_writes_or_db_mutations(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo, "2026-08-07")
    csv_before = csv_path.read_bytes()

    result = sync_parquet_date(repo, "2026-08-07", dry_run=True)

    assert result.rebuilt_or_written is False
    assert not parquet_partition_path(
        tmp_path / "data" / "parquet", date(2026, 8, 7)
    ).exists()
    assert repo.get_parquet_export("2026-08-07") is None
    assert csv_path.read_bytes() == csv_before


# --- Apply Tests ---


def test_apply_create_produces_valid_current_partition_and_db_record(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False)

    assert result.rebuilt_or_written is True
    assert result.export_status_after is ParquetExportStatus.CURRENT
    assert result.parquet_path is not None and result.parquet_path.exists()

    db_rec = repo.get_parquet_export("2026-08-07")
    assert db_rec is not None
    assert db_rec.status is ParquetExportStatus.CURRENT


def test_apply_stale_rebuild_produces_current(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    new_rows = (_row("AAA", 1), _row("XYZ", 2))
    new_content = canonical_csv_bytes(new_rows)
    csv_path.write_bytes(new_content)
    new_checksum = hashlib.sha256(new_content).hexdigest()

    with sqlite3.connect(repo.database_path) as conn:
        conn.execute(
            """
            UPDATE date_sync_state
            SET csv_checksum_sha256 = ?, valid_row_count = 2
            WHERE market_date = '2026-08-07'
            """,
            (new_checksum,),
        )

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False)
    assert result.action is ParquetExportAction.REBUILD_STALE
    assert result.rebuilt_or_written is True
    assert result.export_status_after is ParquetExportStatus.CURRENT


def test_apply_corrupt_rebuild_produces_current(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    parquet_path = parquet_partition_path(
        tmp_path / "data" / "parquet", date(2026, 8, 7)
    )
    parquet_path.parent.mkdir(parents=True)
    parquet_path.write_bytes(b"corrupt")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False)
    assert result.action is ParquetExportAction.REBUILD_CORRUPT
    assert result.rebuilt_or_written is True
    assert result.export_status_after is ParquetExportStatus.CURRENT


def test_apply_reindex_current_creates_db_record_without_writing_file(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    # Delete DB record
    with sqlite3.connect(repo.database_path) as conn:
        conn.execute("DELETE FROM parquet_exports")

    parquet_path = parquet_partition_path(
        tmp_path / "data" / "parquet", date(2026, 8, 7)
    )
    mtime_before = parquet_path.stat().st_mtime_ns

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False)
    assert result.action is ParquetExportAction.REINDEX_CURRENT
    assert result.rebuilt_or_written is False
    assert parquet_path.stat().st_mtime_ns == mtime_before

    db_rec = repo.get_parquet_export("2026-08-07")
    assert db_rec is not None
    assert db_rec.status is ParquetExportStatus.CURRENT


def test_one_date_failure_does_not_prevent_another_date_succeeding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")
    seed_verified_date(repo, "2026-08-08")

    from psx_data_sync import parquet_sync

    original_write = parquet_sync.write_parquet_partition

    def failing_write(market_date, source_csv_path, output_root):
        if market_date == date(2026, 8, 7):
            raise OSError("simulated disk full")
        return original_write(market_date, source_csv_path, output_root)

    monkeypatch.setattr(parquet_sync, "write_parquet_partition", failing_write)

    range_res = sync_parquet_range(
        repo, "2026-08-07", "2026-08-08", dry_run=False
    )
    assert len(range_res.results) == 2

    res_1 = range_res.results[0]
    res_2 = range_res.results[1]

    assert res_1.action is ParquetExportAction.FAILED
    assert res_1.error == "simulated disk full"

    assert res_2.action is ParquetExportAction.CREATE
    assert res_2.export_status_after is ParquetExportStatus.CURRENT


# --- No-op & Recovery & Forced ---


def test_second_identical_apply_performs_zero_writes_and_rebuilds(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    first = sync_parquet_range(repo, "2026-08-07", "2026-08-07", dry_run=False)
    assert first.written_or_rebuilt_count == 1

    second = sync_parquet_range(repo, "2026-08-07", "2026-08-07", dry_run=False)
    assert second.written_or_rebuilt_count == 0
    assert second.current_count == 1
    assert second.results[0].action is ParquetExportAction.NO_ACTION


def test_valid_artifact_exists_missing_db_record_reindexes_without_rebuild(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    with sqlite3.connect(repo.database_path) as conn:
        conn.execute("DELETE FROM parquet_exports")

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False)
    assert result.action is ParquetExportAction.REINDEX_CURRENT
    assert result.rebuilt_or_written is False
    assert repo.get_parquet_export("2026-08-07") is not None


def test_forced_rebuild_regenerates_current_partition(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False, rebuild=True)
    assert result.action is ParquetExportAction.REBUILD_FORCED
    assert result.rebuilt_or_written is True
    assert result.export_status_after is ParquetExportStatus.CURRENT


def test_rebuild_false_leaves_current_partition_untouched(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")
    sync_parquet_date(repo, "2026-08-07", dry_run=False)

    result = sync_parquet_date(repo, "2026-08-07", dry_run=False, rebuild=False)
    assert result.action is ParquetExportAction.NO_ACTION
    assert result.rebuilt_or_written is False


# --- Completeness & Canonical Safety ---


def test_completeness_calculation(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07")

    # 2026-08-08: confirmed non-trading
    run_id = repo.begin_sync_run("fetch", "2026-08-08", "2026-08-08", 1, 1)
    repo.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date="2026-08-08",
            attempt_number=1,
            started_at="2026-08-08T10:00:00+00:00",
            finished_at="2026-08-08T10:00:01+00:00",
            duration_ms=500.0,
            http_status=200,
            response_bytes=100,
            response_classification="EMPTY_MARKET_RESPONSE",
            final_status=DownloadStatus.CONFIRMED_NON_TRADING,
            retryable=False,
        ),
    )

    result = sync_parquet_range(repo, "2026-08-07", "2026-08-08", dry_run=False)
    assert result.synchronized is True
    assert result.synchronization_percentage == 100.0


def test_canonical_csv_hashes_unchanged_after_apply_and_rebuild(
    tmp_path: Path,
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo, "2026-08-07")
    hash_before = csv_path.read_bytes()

    sync_parquet_date(repo, "2026-08-07", dry_run=False)
    assert csv_path.read_bytes() == hash_before

    sync_parquet_date(repo, "2026-08-07", dry_run=False, rebuild=True)
    assert csv_path.read_bytes() == hash_before
