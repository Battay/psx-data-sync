from __future__ import annotations

import hashlib
import sqlite3
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from psx_data_sync.exporter import canonical_csv_bytes
from psx_data_sync.parquet_store import consolidated_parquet_path
from psx_data_sync.parquet_sync import ParquetExportAction, sync_consolidated_parquet
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
    raw_dir = repo.raw_output_dir
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


def _update_verified_source(
    repo: StateRepository, market_date: str, rows: tuple[ValidEquityRow, ...]
) -> None:
    path = repo.raw_output_dir / f"market_{market_date}.csv"
    content = canonical_csv_bytes(rows)
    path.write_bytes(content)
    checksum = hashlib.sha256(content).hexdigest()
    with sqlite3.connect(repo.database_path) as connection:
        connection.execute(
            "UPDATE date_sync_state SET csv_checksum_sha256=?, valid_row_count=?, "
            "parsed_row_count=?, record_updated_at=record_updated_at WHERE market_date=?",
            (checksum, len(rows), len(rows), market_date),
        )


def test_dry_run_reports_missing_and_writes_nothing(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    csv_path = seed_verified_date(repo)
    before = csv_path.read_bytes()

    result = sync_consolidated_parquet(repo, dry_run=True)

    assert result.status is ParquetExportStatus.MISSING
    assert result.action is ParquetExportAction.CREATE
    assert result.source_dates == 1
    assert result.source_rows == 2
    assert result.rows_written == 0
    assert not result.output_path.exists()
    assert csv_path.read_bytes() == before


def test_apply_builds_one_consolidated_file_from_multiple_dates(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-08", (_row("ZZZ", 1), _row("AAA", 2)))
    seed_verified_date(repo, "2026-08-07", (_row("MEBL", 1),))

    result = sync_consolidated_parquet(repo, dry_run=False)

    assert result.status is ParquetExportStatus.CURRENT
    assert result.source_dates == 2
    assert result.source_rows == result.rows_written == 3
    assert result.output_path == tmp_path / "data" / "parquet" / "market.parquet"
    assert list((tmp_path / "data" / "parquet").rglob("*.parquet")) == [result.output_path]
    table = pq.read_table(result.output_path)
    assert table["symbol"].to_pylist() == ["MEBL", "AAA", "ZZZ"]


def test_second_scan_is_current_with_same_identity(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo)
    built = sync_consolidated_parquet(repo, dry_run=False)
    mtime = built.output_path.stat().st_mtime_ns

    current = sync_consolidated_parquet(repo, dry_run=False)

    assert current.status is ParquetExportStatus.CURRENT
    assert current.action is ParquetExportAction.NO_ACTION
    assert current.source_identity == built.source_identity
    assert current.rows_written == 0
    assert current.output_path.stat().st_mtime_ns == mtime


def test_stale_when_verified_csv_is_added(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07", (_row("AAA"),))
    built = sync_consolidated_parquet(repo, dry_run=False)
    seed_verified_date(repo, "2026-08-08", (_row("BBB"),))

    stale = sync_consolidated_parquet(repo, dry_run=True)
    assert stale.status is ParquetExportStatus.STALE
    assert stale.action is ParquetExportAction.REBUILD_STALE
    assert stale.source_identity != built.source_identity


def test_stale_when_verified_csv_changes(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07", (_row("AAA"),))
    built = sync_consolidated_parquet(repo, dry_run=False)
    _update_verified_source(repo, "2026-08-07", (_row("AAA"), _row("BBB", 2)))

    stale = sync_consolidated_parquet(repo, dry_run=True)
    assert stale.status is ParquetExportStatus.STALE
    assert stale.source_rows == 2
    assert stale.source_identity != built.source_identity


def test_stale_when_verified_source_is_removed(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07", (_row("AAA"),))
    seed_verified_date(repo, "2026-08-08", (_row("BBB"),))
    built = sync_consolidated_parquet(repo, dry_run=False)
    with sqlite3.connect(repo.database_path) as connection:
        connection.execute(
            "UPDATE date_sync_state SET status='EMPTY_UNRESOLVED' WHERE market_date='2026-08-08'"
        )

    stale = sync_consolidated_parquet(repo, dry_run=True)
    assert stale.status is ParquetExportStatus.STALE
    assert stale.source_dates == 1
    assert stale.source_identity != built.source_identity


def test_stale_when_verified_csv_file_disappears(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    path = seed_verified_date(repo, "2026-08-07", (_row("AAA"),))
    sync_consolidated_parquet(repo, dry_run=False)
    path.unlink()

    stale = sync_consolidated_parquet(repo, dry_run=True)
    assert stale.status is ParquetExportStatus.STALE
    assert stale.source_dates == 1
    assert stale.source_rows == 1
    assert stale.errors
    assert "missing" in stale.errors[0]


def test_corrupt_consolidated_file_is_detected_and_rebuilt(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo)
    path = consolidated_parquet_path(tmp_path / "data" / "parquet")
    path.parent.mkdir(parents=True)
    path.write_bytes(b"corrupt")

    dry = sync_consolidated_parquet(repo, dry_run=True)
    assert dry.status is ParquetExportStatus.CORRUPT
    assert dry.action is ParquetExportAction.REBUILD_CORRUPT

    applied = sync_consolidated_parquet(repo, dry_run=False)
    assert applied.status is ParquetExportStatus.CURRENT
    assert applied.rows_written == 2


def test_legacy_partitions_are_reported_and_untouched(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo)
    legacy = (
        tmp_path / "data" / "parquet" / "market" /
        "market_date=2026-08-07" / "part-0.parquet"
    )
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"legacy bytes are not source input")

    result = sync_consolidated_parquet(repo, dry_run=False)
    assert result.status is ParquetExportStatus.CURRENT
    assert result.legacy_partition_count == 1
    assert legacy.read_bytes() == b"legacy bytes are not source input"
    assert result.output_path.exists()


def test_invalid_verified_source_fails_without_mutating_csv_or_state(tmp_path: Path) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    path = seed_verified_date(repo)
    path.write_bytes(b"tampered")
    state_before = repo.get_date_state("2026-08-07")

    result = sync_consolidated_parquet(repo, dry_run=False)

    assert result.status is ParquetExportStatus.FAILED
    assert result.action is ParquetExportAction.FAILED
    assert result.errors
    assert path.read_bytes() == b"tampered"
    assert repo.get_date_state("2026-08-07") == state_before
    assert not result.output_path.exists()


def test_failed_rebuild_reports_failed_and_preserves_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = make_repository(tmp_path / "state.db", tmp_path)
    seed_verified_date(repo, "2026-08-07", (_row("AAA"),))
    built = sync_consolidated_parquet(repo, dry_run=False)
    before = built.output_path.read_bytes()
    _update_verified_source(repo, "2026-08-07", (_row("AAA"), _row("BBB", 2)))

    from psx_data_sync import parquet_sync

    def fail_build(*args, **kwargs):
        raise OSError("simulated disk full")

    monkeypatch.setattr(parquet_sync, "write_consolidated_parquet", fail_build)
    failed = sync_consolidated_parquet(repo, dry_run=False)

    assert failed.status is ParquetExportStatus.FAILED
    assert failed.action is ParquetExportAction.FAILED
    assert failed.errors == ("simulated disk full",)
    assert built.output_path.read_bytes() == before


def test_custom_raw_output_directory_is_used(tmp_path: Path) -> None:
    repo = StateRepository(
        tmp_path / "state.db",
        project_root=tmp_path / "project",
        raw_output_dir=tmp_path / "custom" / "raw",
    )
    repo.initialize()
    seed_verified_date(repo, "2026-08-20", (_row("OGDC"),))

    result = sync_consolidated_parquet(
        repo,
        output_root=tmp_path / "custom" / "parquet",
        dry_run=False,
    )
    assert result.status is ParquetExportStatus.CURRENT
    assert result.output_path == tmp_path / "custom" / "parquet" / "market.parquet"
    assert repo.get_date_state("2026-08-20").status is PersistentSyncStatus.VERIFIED_TRADING_DATA
