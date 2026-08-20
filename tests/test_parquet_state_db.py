from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from psx_data_sync import __version__
from psx_data_sync.state import (
    DownloadAttemptEvent,
    DownloadStatus,
    ParquetExportStatus,
    PersistentSyncStatus,
)
from psx_data_sync.state_db import (
    SCHEMA_VERSION,
    IncompatibleSchemaError,
    StateDatabaseError,
    StateRepository,
)


def make_repository(database_path: Path, project_root: Path) -> StateRepository:
    return StateRepository(
        database_path,
        project_root=project_root,
        source_endpoint="https://dps.psx.com.pk/historical",
    )


def seed_verified_date(
    repository: StateRepository,
    market_date: str = "2026-08-07",
    checksum: str = "a" * 64,
    valid_rows: int = 100,
) -> None:
    run_id = repository.begin_sync_run("fetch", market_date, market_date, 1, 1)
    repository.record_attempt(
        run_id,
        DownloadAttemptEvent(
            requested_date=market_date,
            attempt_number=1,
            started_at="2026-08-07T10:00:00+00:00",
            finished_at="2026-08-07T10:00:01+00:00",
            duration_ms=1000.0,
            http_status=200,
            response_bytes=5000,
            response_classification="EQUITY_ROWS",
            final_status=DownloadStatus.TRADING_DATA,
            retryable=False,
            parsed_row_count=valid_rows,
            valid_row_count=valid_rows,
            checksum=checksum,
            saved_path=repository.project_root / f"data/raw/market_{market_date}.csv",
        ),
    )


def create_frozen_v2_database(path: Path) -> None:
    """Create a database frozen at schema version 2 for migration testing."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(
            """
            CREATE TABLE sync_schema_metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                application_version TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO sync_schema_metadata VALUES "
            "(1, 2, '2026-08-01T00:00:00', '2026-08-01T00:00:00', '0.4.0')"
        )
        conn.execute(
            """
            CREATE TABLE date_sync_state (
                market_date TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                evidence_state TEXT NOT NULL DEFAULT 'NONE',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                successful_attempt_count INTEGER NOT NULL DEFAULT 0,
                last_http_status INTEGER,
                last_response_bytes INTEGER NOT NULL DEFAULT 0,
                parsed_row_count INTEGER NOT NULL DEFAULT 0,
                valid_row_count INTEGER NOT NULL DEFAULT 0,
                rejected_row_count INTEGER NOT NULL DEFAULT 0,
                csv_checksum_sha256 TEXT,
                csv_relative_path TEXT,
                first_attempt_at TEXT,
                last_attempt_at TEXT,
                last_success_at TEXT,
                last_verified_at TEXT,
                last_error_type TEXT,
                last_error_message TEXT,
                last_duration_ms REAL,
                source_endpoint TEXT NOT NULL,
                classification_policy_version TEXT,
                classification_basis TEXT,
                classification_updated_at TEXT,
                next_recheck_after TEXT,
                recheck_policy_version TEXT,
                record_created_at TEXT NOT NULL,
                record_updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO date_sync_state (
                market_date, status, evidence_state, csv_checksum_sha256, valid_row_count,
                source_endpoint, record_created_at, record_updated_at
            ) VALUES (
                '2026-08-07', 'VERIFIED_TRADING_DATA', 'LOCAL_CSV_SHA256_VERIFIED',
                '{"a" * 64}', 100, 'https://dps.psx.com.pk/historical',
                '2026-08-07T00:00:00', '2026-08-07T00:00:00'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE sync_runs (
                run_id TEXT PRIMARY KEY,
                command_type TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                requested_date_count INTEGER NOT NULL,
                worker_count INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_ms REAL,
                completed_count INTEGER NOT NULL DEFAULT 0,
                network_fetch_count INTEGER NOT NULL DEFAULT 0,
                local_skip_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                unresolved_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                total_valid_rows INTEGER NOT NULL DEFAULT 0,
                total_rejected_rows INTEGER NOT NULL DEFAULT 0,
                total_response_bytes INTEGER NOT NULL DEFAULT 0,
                total_attempts INTEGER NOT NULL DEFAULT 0,
                interrupted INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                application_version TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE download_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                market_date TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                http_status INTEGER,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                response_classification TEXT,
                final_status TEXT NOT NULL,
                retryable INTEGER NOT NULL DEFAULT 0,
                error_type TEXT,
                error_message TEXT,
                parsed_row_count INTEGER NOT NULL DEFAULT 0,
                valid_row_count INTEGER NOT NULL DEFAULT 0,
                rejected_row_count INTEGER NOT NULL DEFAULT 0,
                checksum TEXT,
                csv_relative_path TEXT,
                worker_identifier TEXT,
                created_at TEXT NOT NULL,
                UNIQUE (run_id, market_date, attempt_number),
                FOREIGN KEY (run_id) REFERENCES sync_runs(run_id),
                FOREIGN KEY (market_date) REFERENCES date_sync_state(market_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE sync_run_date_results (
                run_id TEXT NOT NULL,
                market_date TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts_in_run INTEGER NOT NULL,
                local_skip INTEGER NOT NULL DEFAULT 0,
                parsed_row_count INTEGER NOT NULL DEFAULT 0,
                valid_row_count INTEGER NOT NULL DEFAULT 0,
                rejected_row_count INTEGER NOT NULL DEFAULT 0,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                checksum TEXT,
                csv_relative_path TEXT,
                duration_ms REAL NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, market_date),
                FOREIGN KEY (run_id) REFERENCES sync_runs(run_id),
                FOREIGN KEY (market_date) REFERENCES date_sync_state(market_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE reconciliation_runs (
                run_id TEXT PRIMARY KEY,
                policy_version TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                mode TEXT NOT NULL,
                requested_date_count INTEGER NOT NULL,
                worker_count INTEGER NOT NULL,
                force_recheck INTEGER NOT NULL DEFAULT 0,
                max_rechecks_per_date INTEGER NOT NULL DEFAULT 1,
                cooldown_seconds REAL NOT NULL DEFAULT 86400,
                verified_count INTEGER NOT NULL DEFAULT 0,
                confirmed_non_trading_count INTEGER NOT NULL DEFAULT 0,
                never_attempted_count INTEGER NOT NULL DEFAULT 0,
                unresolved_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                file_health_issue_count INTEGER NOT NULL DEFAULT 0,
                network_recheck_planned_count INTEGER NOT NULL DEFAULT 0,
                network_recheck_count INTEGER NOT NULL DEFAULT 0,
                local_repair_count INTEGER NOT NULL DEFAULT 0,
                manual_review_count INTEGER NOT NULL DEFAULT 0,
                status_transition_count INTEGER NOT NULL DEFAULT 0,
                complete INTEGER NOT NULL DEFAULT 0,
                linked_sync_run_id TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_ms REAL,
                interrupted INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                error_message TEXT,
                application_version TEXT NOT NULL,
                FOREIGN KEY (linked_sync_run_id) REFERENCES sync_runs(run_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE reconciliation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                market_date TEXT NOT NULL,
                previous_status TEXT NOT NULL,
                new_status TEXT NOT NULL,
                action TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                evidence_classification TEXT NOT NULL,
                evidence_summary TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES reconciliation_runs(run_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE repair_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reconciliation_run_id TEXT NOT NULL,
                market_date TEXT NOT NULL,
                staged_relative_path TEXT NOT NULL,
                prior_checksum_sha256 TEXT,
                candidate_checksum_sha256 TEXT,
                prior_row_count INTEGER,
                candidate_row_count INTEGER,
                validation_state TEXT NOT NULL,
                disposition TEXT NOT NULL,
                created_at TEXT NOT NULL,
                evaluated_at TEXT,
                promoted_at TEXT,
                message TEXT,
                UNIQUE (reconciliation_run_id, market_date),
                FOREIGN KEY (reconciliation_run_id)
                    REFERENCES reconciliation_runs(run_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE reconciliation_recheck_claims (
                market_date TEXT PRIMARY KEY,
                reconciliation_run_id TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (reconciliation_run_id)
                    REFERENCES reconciliation_runs(run_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX idx_date_sync_state_status ON date_sync_state(status)"
        )
        conn.execute(
            "CREATE INDEX idx_download_attempts_market_date "
            "ON download_attempts(market_date, created_at)"
        )
        conn.execute(
            "CREATE INDEX idx_download_attempts_run_id "
            "ON download_attempts(run_id)"
        )
        conn.execute(
            "CREATE INDEX idx_sync_run_date_results_market_date "
            "ON sync_run_date_results(market_date)"
        )
        conn.execute(
            "CREATE INDEX idx_sync_runs_started_at ON sync_runs(started_at)"
        )
        conn.execute(
            "CREATE INDEX idx_reconciliation_runs_started_at "
            "ON reconciliation_runs(started_at)"
        )
        conn.execute(
            "CREATE INDEX idx_reconciliation_events_market_date "
            "ON reconciliation_events(market_date, created_at)"
        )
        conn.execute(
            "CREATE INDEX idx_reconciliation_events_run_id "
            "ON reconciliation_events(run_id)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX idx_reconciliation_events_unique_decision "
            "ON reconciliation_events(run_id, market_date, action, new_status)"
        )
        conn.execute(
            "CREATE INDEX idx_download_attempts_evidence "
            "ON download_attempts(market_date, response_classification, final_status, finished_at)"
        )
        conn.execute(
            "CREATE INDEX idx_repair_candidates_market_date "
            "ON repair_candidates(market_date, created_at)"
        )
        conn.execute(
            "CREATE INDEX idx_recheck_claims_expires_at "
            "ON reconciliation_recheck_claims(expires_at)"
        )
        conn.commit()


def test_fresh_schema_v3_initialization(tmp_path: Path) -> None:
    db = tmp_path / "fresh_v3.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    assert repo.verify_schema() == SCHEMA_VERSION == 3
    tables, indexes = repo.schema_objects()
    assert "parquet_exports" in tables
    assert "idx_parquet_exports_status" in indexes


def test_parquet_exports_exact_expected_shape(tmp_path: Path) -> None:
    db = tmp_path / "shape.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        columns = {
            row["name"]: (row["type"], row["notnull"], row["pk"])
            for row in conn.execute("PRAGMA table_info(parquet_exports)")
        }

    expected_cols = {
        "market_date": ("TEXT", 0, 1),
        "status": ("TEXT", 1, 0),
        "schema_version": ("TEXT", 1, 0),
        "source_csv_checksum_sha256": ("TEXT", 1, 0),
        "source_row_count": ("INTEGER", 1, 0),
        "parquet_relative_path": ("TEXT", 0, 0),
        "parquet_checksum_sha256": ("TEXT", 0, 0),
        "parquet_row_count": ("INTEGER", 0, 0),
        "exporter_version": ("TEXT", 1, 0),
        "created_at": ("TEXT", 1, 0),
        "updated_at": ("TEXT", 1, 0),
        "verified_at": ("TEXT", 0, 0),
        "last_error": ("TEXT", 0, 0),
    }
    assert columns == expected_cols


def test_parquet_exports_fk_and_index_verification(tmp_path: Path) -> None:
    db = tmp_path / "fk_idx.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    with sqlite3.connect(db) as conn:
        fks = [
            (row[2], row[3], row[4])
            for row in conn.execute("PRAGMA foreign_key_list(parquet_exports)")
        ]
        assert ("date_sync_state", "market_date", "market_date") in fks

        indexes = [
            row[1]
            for row in conn.execute("PRAGMA index_list(parquet_exports)")
        ]
        assert "idx_parquet_exports_status" in indexes


def test_valid_export_state_round_trip(tmp_path: Path) -> None:
    db = tmp_path / "roundtrip.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    parquet_file = tmp_path / "parquet" / "market_date=2026-08-07" / "part-0.parquet"
    record = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.CURRENT,
        schema_version="psx_market_parquet_schema_v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        parquet_path=parquet_file,
        parquet_checksum_sha256="b" * 64,
        parquet_row_count=100,
        verified_at="2026-08-07T10:00:00Z",
    )

    assert record.market_date == "2026-08-07"
    assert record.status is ParquetExportStatus.CURRENT
    assert record.schema_version == "psx_market_parquet_schema_v1"
    assert record.source_csv_checksum_sha256 == "a" * 64
    assert record.source_row_count == 100
    assert record.parquet_checksum_sha256 == "b" * 64
    assert record.parquet_row_count == 100
    assert record.exporter_version == __version__
    assert record.verified_at == "2026-08-07T10:00:00Z"

    fetched = repo.get_parquet_export("2026-08-07")
    assert fetched == record


def test_created_at_preserved_across_upsert(tmp_path: Path) -> None:
    db = tmp_path / "created_at.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    parquet_file = tmp_path / "parquet" / "market_date=2026-08-07" / "part-0.parquet"
    first = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.CURRENT,
        schema_version="psx_market_parquet_schema_v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        parquet_path=parquet_file,
        parquet_checksum_sha256="b" * 64,
        parquet_row_count=100,
        verified_at="2026-08-07T10:00:00Z",
    )

    second = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.STALE,
        schema_version="psx_market_parquet_schema_v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        last_error="stale source",
    )

    assert second.created_at == first.created_at
    assert second.status is ParquetExportStatus.STALE
    assert second.last_error == "stale source"


def test_sorted_range_lookup(tmp_path: Path) -> None:
    db = tmp_path / "range.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    for day in ("2026-08-07", "2026-08-08", "2026-08-09"):
        seed_verified_date(repo, day, checksum="a" * 64, valid_rows=50)
        repo.upsert_parquet_export(
            day,
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=50,
            parquet_path=tmp_path / f"parquet/{day}.parquet",
            parquet_checksum_sha256="b" * 64,
            parquet_row_count=50,
            verified_at="2026-08-07T10:00:00Z",
        )

    results = repo.get_parquet_exports_for_range("2026-08-07", "2026-08-09")
    assert len(results) == 3
    assert [r.market_date for r in results] == [
        "2026-08-07",
        "2026-08-08",
        "2026-08-09",
    ]


def test_untracked_date_rejected(tmp_path: Path) -> None:
    db = tmp_path / "untracked.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    with pytest.raises(StateDatabaseError, match="cannot export untracked date"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
        )


def test_non_verified_date_rejected(tmp_path: Path) -> None:
    db = tmp_path / "non_verified.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            INSERT INTO date_sync_state (
                market_date, status, evidence_state, source_endpoint,
                record_created_at, record_updated_at
            ) VALUES ('2026-08-07', 'EMPTY_UNRESOLVED', 'NONE', 'url', 'now', 'now')
            """
        )

    with pytest.raises(StateDatabaseError, match="must be VERIFIED_TRADING_DATA"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
        )


def test_source_checksum_mismatch_rejected(tmp_path: Path) -> None:
    db = tmp_path / "checksum_mismatch.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="checksum mismatch"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="b" * 64,
            source_row_count=100,
        )


def test_source_row_count_mismatch_rejected(tmp_path: Path) -> None:
    db = tmp_path / "row_count_mismatch.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()

    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="row count mismatch"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=999,
        )


def test_current_rejects_missing_path(tmp_path: Path) -> None:
    db = tmp_path / "curr_path.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="requires parquet_path"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=None,
            parquet_checksum_sha256="b" * 64,
            parquet_row_count=100,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_current_rejects_missing_checksum(tmp_path: Path) -> None:
    db = tmp_path / "curr_chk.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="requires parquet_checksum_sha256"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=tmp_path / "part.parquet",
            parquet_checksum_sha256=None,
            parquet_row_count=100,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_current_rejects_missing_row_count(tmp_path: Path) -> None:
    db = tmp_path / "curr_cnt.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="requires parquet_row_count"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=tmp_path / "part.parquet",
            parquet_checksum_sha256="b" * 64,
            parquet_row_count=None,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_current_rejects_parquet_source_row_count_mismatch(tmp_path: Path) -> None:
    db = tmp_path / "curr_mismatch.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="row count mismatch"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=tmp_path / "part.parquet",
            parquet_checksum_sha256="b" * 64,
            parquet_row_count=50,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_current_rejects_missing_verified_at(tmp_path: Path) -> None:
    db = tmp_path / "curr_ver.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="requires verified_at timestamp"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CURRENT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=tmp_path / "part.parquet",
            parquet_checksum_sha256="b" * 64,
            parquet_row_count=100,
            verified_at=None,
        )


def test_valid_current_succeeds(tmp_path: Path) -> None:
    db = tmp_path / "curr_ok.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    record = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.CURRENT,
        schema_version="v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        parquet_path=tmp_path / "part.parquet",
        parquet_checksum_sha256="b" * 64,
        parquet_row_count=100,
        verified_at="2026-08-07T10:00:00Z",
    )
    assert record.status is ParquetExportStatus.CURRENT
    assert record.verified_at == "2026-08-07T10:00:00Z"


def test_missing_rejects_artifact_metadata(tmp_path: Path) -> None:
    db = tmp_path / "missing_meta.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="cannot have parquet artifact metadata"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            parquet_path=tmp_path / "part.parquet",
        )


def test_missing_rejects_verified_at(tmp_path: Path) -> None:
    db = tmp_path / "missing_ver.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="cannot have verified_at timestamp"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.MISSING,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_valid_missing_succeeds(tmp_path: Path) -> None:
    db = tmp_path / "missing_ok.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    record = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.MISSING,
        schema_version="v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
    )
    assert record.status is ParquetExportStatus.MISSING
    assert record.parquet_relative_path is None
    assert record.verified_at is None


def test_stale_may_retain_artifact_metadata(tmp_path: Path) -> None:
    db = tmp_path / "stale_meta.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    record = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.STALE,
        schema_version="v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        parquet_path=tmp_path / "part.parquet",
        parquet_checksum_sha256="old",
        parquet_row_count=50,
    )
    assert record.status is ParquetExportStatus.STALE
    assert record.parquet_checksum_sha256 == "old"
    assert record.verified_at is None


def test_stale_rejects_verified_at(tmp_path: Path) -> None:
    db = tmp_path / "stale_ver.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="cannot have verified_at timestamp"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.STALE,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_corrupt_may_retain_artifact_metadata(tmp_path: Path) -> None:
    db = tmp_path / "corrupt_meta.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    record = repo.upsert_parquet_export(
        "2026-08-07",
        status=ParquetExportStatus.CORRUPT,
        schema_version="v1",
        source_csv_checksum_sha256="a" * 64,
        source_row_count=100,
        parquet_path=tmp_path / "part.parquet",
        parquet_checksum_sha256="bad_sha",
        parquet_row_count=0,
        last_error="Corrupt Parquet file header",
    )
    assert record.status is ParquetExportStatus.CORRUPT
    assert record.parquet_checksum_sha256 == "bad_sha"
    assert record.verified_at is None


def test_corrupt_rejects_verified_at(tmp_path: Path) -> None:
    db = tmp_path / "corrupt_ver.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="cannot have verified_at timestamp"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.CORRUPT,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            verified_at="2026-08-07T10:00:00Z",
        )


def test_failed_requires_last_error(tmp_path: Path) -> None:
    db = tmp_path / "failed_err.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="requires last_error"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.FAILED,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            last_error=None,
        )


def test_failed_rejects_verified_at(tmp_path: Path) -> None:
    db = tmp_path / "failed_ver.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="cannot have verified_at timestamp"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status=ParquetExportStatus.FAILED,
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
            last_error="write failed",
            verified_at="2026-08-07T10:00:00Z",
        )


def test_invalid_status_rejected(tmp_path: Path) -> None:
    db = tmp_path / "invalid_status.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    seed_verified_date(repo, "2026-08-07", checksum="a" * 64, valid_rows=100)

    with pytest.raises(StateDatabaseError, match="invalid parquet export status"):
        repo.upsert_parquet_export(
            "2026-08-07",
            status="NON_EXISTENT_STATUS",
            schema_version="v1",
            source_csv_checksum_sha256="a" * 64,
            source_row_count=100,
        )


def test_v2_to_v3_migration_preserves_all_existing_data(tmp_path: Path) -> None:
    db = tmp_path / "v2_migration.db"
    create_frozen_v2_database(db)

    repo = make_repository(db, tmp_path)
    repo.initialize()

    assert repo.verify_schema() == 3
    date_state = repo.get_date_state("2026-08-07")
    assert date_state is not None
    assert date_state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert date_state.valid_row_count == 100

    tables, indexes = repo.schema_objects()
    assert "parquet_exports" in tables
    assert "idx_parquet_exports_status" in indexes


def test_v1_to_v3_migration_still_works(tmp_path: Path) -> None:
    from tests.test_reconciliation_state_db import create_frozen_v1_database

    db = tmp_path / "v1_migration.db"
    create_frozen_v1_database(db)

    repo = make_repository(db, tmp_path)
    repo.initialize()

    assert repo.verify_schema() == 3
    tables, _ = repo.schema_objects()
    assert "parquet_exports" in tables


def test_initialization_idempotency(tmp_path: Path) -> None:
    db = tmp_path / "idempotent.db"
    repo = make_repository(db, tmp_path)
    repo.initialize()
    repo.initialize()
    repo.initialize()

    assert repo.verify_schema() == 3


def test_injected_migration_failure_rolls_back_v3_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = tmp_path / "failed_migration.db"
    create_frozen_v2_database(db)

    repo = make_repository(db, tmp_path)

    class FailingMigrationConnection(sqlite3.Connection):
        def execute(self, sql: str, parameters=(), /):
            if "CREATE TABLE IF NOT EXISTS parquet_exports" in sql:
                raise sqlite3.OperationalError("injected migration failure")
            return super().execute(sql, parameters)

    def failing_connect() -> sqlite3.Connection:
        connection = sqlite3.connect(
            db,
            timeout=30.0,
            factory=FailingMigrationConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    monkeypatch.setattr(repo, "_connect", failing_connect)

    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        repo.initialize()

    with sqlite3.connect(db) as conn:
        version = conn.execute(
            "SELECT schema_version FROM sync_schema_metadata"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert version == 2
    assert "parquet_exports" not in tables


def test_future_schema_version_rejected_without_mutation(tmp_path: Path) -> None:
    db = tmp_path / "future.db"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE sync_schema_metadata (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                application_version TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO sync_schema_metadata VALUES (1, 99, 'now', 'now', '99.0')"
        )

    repo = make_repository(db, tmp_path)
    with pytest.raises(IncompatibleSchemaError, match="version 99"):
        repo.initialize()

    with sqlite3.connect(db) as conn:
        version = conn.execute(
            "SELECT schema_version FROM sync_schema_metadata"
        ).fetchone()[0]
    assert version == 99
