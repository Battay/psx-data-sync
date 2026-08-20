from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

from psx_data_sync.state import (
    AttemptEvidenceRecord,
    ChecksumState,
    DateReconciliationResult,
    DownloadAttemptEvent,
    DownloadStatus,
    FileHealthState,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationEvidenceSummary,
    ReconciliationMode,
    ReconciliationRangeResult,
    ReconciliationRunStatus,
    SyncEvidenceState,
    WEEKEND_EMPTY_CLASSIFICATION_BASIS,
)
from psx_data_sync.state_db import (
    EXPECTED_DATE_STATE_COLUMNS,
    EXPECTED_INDEXES,
    EXPECTED_TABLES,
    SCHEMA_VERSION,
    IncompatibleSchemaError,
    StateDatabaseError,
    StateRepository,
)


POLICY_VERSION = "psx_reconciliation_policy_v1"
MARKET_DATE = "2026-08-08"
V1_CREATED_AT = "2026-08-08T08:00:00.000000+00:00"
V1_UPDATED_AT = "2026-08-08T08:01:00.000000+00:00"
V1_RUN_ID = "frozen-v1-run"

REQUIRED_D4_TABLES = {
    "reconciliation_runs",
    "reconciliation_events",
    "repair_candidates",
    "reconciliation_recheck_claims",
}
REQUIRED_D4_INDEXES = {
    "idx_reconciliation_runs_started_at",
    "idx_reconciliation_events_market_date",
    "idx_reconciliation_events_run_id",
    "idx_download_attempts_evidence",
    "idx_repair_candidates_market_date",
    "idx_recheck_claims_expires_at",
}
REQUIRED_D4_DATE_COLUMNS = {
    "classification_policy_version",
    "classification_basis",
    "classification_updated_at",
    "next_recheck_after",
    "recheck_policy_version",
}


# This is a literal copy of schema version 1. It deliberately does not call the
# current initializer, so migration tests cannot pass merely because fresh-v2
# creation and v1 migration share the same defect.
FROZEN_V1_SCHEMA = """
CREATE TABLE sync_schema_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    application_version TEXT NOT NULL
);

CREATE TABLE date_sync_state (
    market_date TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    evidence_state TEXT NOT NULL DEFAULT 'NONE',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    successful_attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK (successful_attempt_count >= 0),
    last_http_status INTEGER,
    last_response_bytes INTEGER NOT NULL DEFAULT 0
        CHECK (last_response_bytes >= 0),
    parsed_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (parsed_row_count >= 0),
    valid_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (valid_row_count >= 0),
    rejected_row_count INTEGER NOT NULL DEFAULT 0
        CHECK (rejected_row_count >= 0),
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
    record_created_at TEXT NOT NULL,
    record_updated_at TEXT NOT NULL
);

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
);

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
);

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
);

CREATE INDEX idx_date_sync_state_status ON date_sync_state(status);
CREATE INDEX idx_download_attempts_market_date
    ON download_attempts(market_date, created_at);
CREATE INDEX idx_download_attempts_run_id ON download_attempts(run_id);
CREATE INDEX idx_sync_run_date_results_market_date
    ON sync_run_date_results(market_date);
CREATE INDEX idx_sync_runs_started_at ON sync_runs(started_at);
"""

V1_DATE_STATE_COLUMNS = (
    "market_date",
    "status",
    "evidence_state",
    "attempt_count",
    "successful_attempt_count",
    "last_http_status",
    "last_response_bytes",
    "parsed_row_count",
    "valid_row_count",
    "rejected_row_count",
    "csv_checksum_sha256",
    "csv_relative_path",
    "first_attempt_at",
    "last_attempt_at",
    "last_success_at",
    "last_verified_at",
    "last_error_type",
    "last_error_message",
    "last_duration_ms",
    "source_endpoint",
    "record_created_at",
    "record_updated_at",
)


def create_frozen_v1_database(path: Path, *, schema_version: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(FROZEN_V1_SCHEMA)
        connection.execute(
            "INSERT INTO sync_schema_metadata VALUES (1, ?, ?, ?, ?)",
            (
                schema_version,
                V1_CREATED_AT,
                V1_UPDATED_AT,
                "0.3.0",
            ),
        )
        connection.execute(
            """
            INSERT INTO date_sync_state VALUES (
                ?, 'VERIFIED_TRADING_DATA', 'NETWORK_VALIDATED_CSV',
                1, 1, 200, 1234, 2, 2, 0, ?, ?,
                ?, ?, ?, ?, NULL, NULL, 42.5, ?, ?, ?
            )
            """,
            (
                MARKET_DATE,
                "a" * 64,
                "data/raw/market_2026-08-08.csv",
                V1_CREATED_AT,
                V1_UPDATED_AT,
                V1_UPDATED_AT,
                V1_UPDATED_AT,
                "https://dps.psx.com.pk/historical",
                V1_CREATED_AT,
                V1_UPDATED_AT,
            ),
        )
        connection.execute(
            """
            INSERT INTO sync_runs VALUES (
                ?, 'range', ?, ?, 1, 3, ?, ?, 100.0,
                1, 1, 0, 1, 0, 0, 2, 0, 1234, 1, 0,
                'COMPLETED', '0.3.0'
            )
            """,
            (
                V1_RUN_ID,
                MARKET_DATE,
                MARKET_DATE,
                V1_CREATED_AT,
                V1_UPDATED_AT,
            ),
        )
        connection.execute(
            """
            INSERT INTO download_attempts (
                run_id, market_date, attempt_number, started_at, finished_at,
                duration_ms, http_status, response_bytes,
                response_classification, final_status, retryable,
                error_type, error_message, parsed_row_count, valid_row_count,
                rejected_row_count, checksum, csv_relative_path,
                worker_identifier, created_at
            ) VALUES (?, ?, 1, ?, ?, 42.5, 200, 1234, 'EQUITY_ROWS',
                      'TRADING_DATA', 0, NULL, NULL, 2, 2, 0, ?, ?,
                      'worker-v1', ?)
            """,
            (
                V1_RUN_ID,
                MARKET_DATE,
                V1_CREATED_AT,
                V1_UPDATED_AT,
                "a" * 64,
                "data/raw/market_2026-08-08.csv",
                V1_UPDATED_AT,
            ),
        )
        connection.execute(
            """
            INSERT INTO sync_run_date_results VALUES (
                ?, ?, 'TRADING_DATA', 1, 0, 2, 2, 0, 1234, ?, ?,
                42.5, NULL, ?
            )
            """,
            (
                V1_RUN_ID,
                MARKET_DATE,
                "a" * 64,
                "data/raw/market_2026-08-08.csv",
                V1_UPDATED_AT,
            ),
        )


def make_repository(path: Path, project_root: Path) -> StateRepository:
    return StateRepository(path, project_root=project_root)


def make_attempt(
    *,
    market_date: str = MARKET_DATE,
    attempt_number: int,
    finished_at: str,
    http_status: int | None,
    classification: str | None,
    status: DownloadStatus,
    valid_rows: int = 0,
    checksum: str | None = None,
    saved_path: Path | None = None,
) -> DownloadAttemptEvent:
    started_at = finished_at.replace("01:00:00", "00:59:59")
    return DownloadAttemptEvent(
        requested_date=market_date,
        attempt_number=attempt_number,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=1000.0,
        http_status=http_status,
        response_bytes=100,
        response_classification=classification,
        final_status=status,
        retryable=status is not DownloadStatus.TRADING_DATA,
        parsed_row_count=valid_rows,
        valid_row_count=valid_rows,
        checksum=checksum,
        saved_path=saved_path,
        worker_identifier="evidence-test",
    )


def sample_date_result(
    *,
    previous_status: PersistentSyncStatus = PersistentSyncStatus.EMPTY_UNRESOLVED,
    reconciled_status: PersistentSyncStatus = (
        PersistentSyncStatus.CONFIRMED_NON_TRADING
    ),
    action: ReconciliationAction = ReconciliationAction.CONFIRM_NON_TRADING,
) -> DateReconciliationResult:
    return DateReconciliationResult(
        market_date=MARKET_DATE,
        previous_status=previous_status,
        reconciled_status=reconciled_status,
        policy_version=POLICY_VERSION,
        evidence_classification="REPEATED_EMPTY_WEEKEND",
        action_required=action,
        network_recheck_required=False,
        recheck_eligible_now=False,
        local_repair_required=False,
        evidence_summary=ReconciliationEvidenceSummary(
            weekday="Saturday",
            calendar_weekend=True,
            calendar_support="WEEKEND",
            persistent_evidence="TWO_INDEPENDENT_EMPTY_RUNS",
            http_statuses=(200,),
            response_classifications=("EMPTY_MARKET_RESPONSE",),
            independent_empty_run_count=2,
            independent_valid_run_count=0,
            adjacent_previous_verified=True,
            adjacent_next_verified=True,
            expected_csv_path="data/raw/market_2026-08-08.csv",
            expected_checksum=None,
            observed_checksum=None,
        ),
        attempt_count=2,
        empty_observation_count=2,
        valid_observation_count=0,
        file_state=FileHealthState.NOT_APPLICABLE,
        checksum_state=ChecksumState.NOT_APPLICABLE,
        reasons=("two independent structurally empty observations",),
        warnings=(),
    )


def range_result(
    run_id: str,
    result: DateReconciliationResult,
) -> ReconciliationRangeResult:
    return ReconciliationRangeResult(
        run_id=run_id,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.DRY_RUN,
        policy_version=POLICY_VERSION,
        requested_dates=(MARKET_DATE,),
        results=(result,),
        complete=result.resolved,
        resolution_percentage=100.0 if result.resolved else 0.0,
        counts_by_status={result.reconciled_status: 1},
        counts_by_action={result.action_required: 1},
        verified_count=int(
            result.reconciled_status
            is PersistentSyncStatus.VERIFIED_TRADING_DATA
        ),
        confirmed_non_trading_count=int(
            result.reconciled_status
            is PersistentSyncStatus.CONFIRMED_NON_TRADING
        ),
        never_attempted_count=0,
        unresolved_count=0,
        failure_count=0,
        file_health_issue_count=0,
        network_recheck_planned_count=0,
        network_recheck_count=0,
        local_repair_count=0,
        manual_review_count=0,
        status_transition_count=int(result.previous_status != result.reconciled_status),
        duration_ms=12.5,
    )


def test_frozen_v1_migrates_to_v2_without_losing_history(tmp_path: Path) -> None:
    database = tmp_path / "state" / "frozen-v1.db"
    create_frozen_v1_database(database)

    with sqlite3.connect(database) as connection:
        assert tuple(
            row[1] for row in connection.execute("PRAGMA table_info(date_sync_state)")
        ) == V1_DATE_STATE_COLUMNS

    repository = make_repository(database, tmp_path)
    repository.initialize()

    assert repository.verify_schema() == SCHEMA_VERSION == 3
    state = repository.get_date_state(MARKET_DATE)
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.evidence_state is SyncEvidenceState.NETWORK_VALIDATED_CSV
    assert state.attempt_count == 1
    assert state.csv_checksum_sha256 == "a" * 64
    assert state.csv_relative_path == "data/raw/market_2026-08-08.csv"
    assert state.record_created_at == V1_CREATED_AT
    assert state.record_updated_at == V1_UPDATED_AT
    assert state.classification_policy_version is None
    assert state.classification_basis is None
    assert state.classification_updated_at is None
    assert state.next_recheck_after is None
    assert state.recheck_policy_version is None

    run = repository.get_sync_run(V1_RUN_ID)
    attempts = repository.get_recent_attempts(MARKET_DATE)
    assert run is not None and run.total_attempts == 1
    assert len(attempts) == 1
    assert attempts[0].run_id == V1_RUN_ID
    assert attempts[0].checksum == "a" * 64
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        metadata = connection.execute(
            "SELECT * FROM sync_schema_metadata WHERE singleton = 1"
        ).fetchone()
        result = connection.execute(
            "SELECT * FROM sync_run_date_results WHERE run_id = ?",
            (V1_RUN_ID,),
        ).fetchone()
        assert metadata is not None
        assert metadata["schema_version"] == 3
        assert metadata["created_at"] == V1_CREATED_AT
        assert result is not None and result["checksum"] == "a" * 64
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_v1_migration_and_fresh_v2_initialization_are_idempotent(
    tmp_path: Path,
) -> None:
    migrated_database = tmp_path / "migrated.db"
    fresh_database = tmp_path / "fresh.db"
    create_frozen_v1_database(migrated_database)
    migrated = make_repository(migrated_database, tmp_path)
    fresh = make_repository(fresh_database, tmp_path)

    migrated.initialize()
    fresh.initialize()
    for repository, expected_dates in ((migrated, 1), (fresh, 0)):
        repository.initialize()
        repository.initialize()
        assert repository.verify_schema() == 3
        tables, indexes = repository.schema_objects()
        assert EXPECTED_TABLES <= tables
        assert EXPECTED_INDEXES <= indexes
        assert REQUIRED_D4_TABLES <= tables
        assert REQUIRED_D4_INDEXES <= indexes
        with sqlite3.connect(repository.database_path) as connection:
            counts = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in EXPECTED_TABLES
            }
            assert counts["sync_schema_metadata"] == 1
            assert counts["date_sync_state"] == expected_dates
            assert counts["download_attempts"] == expected_dates
            assert counts["sync_run_date_results"] == expected_dates
            assert counts["sync_runs"] == expected_dates
            assert counts["reconciliation_runs"] == 0
            assert counts["reconciliation_events"] == 0
            assert counts["repair_candidates"] == 0
            assert counts["reconciliation_recheck_claims"] == 0


def test_v2_schema_has_required_columns_indexes_and_foreign_keys(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state.db"
    repository = make_repository(database, tmp_path)
    repository.initialize()

    with sqlite3.connect(database) as connection:
        date_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(date_sync_state)")
        }
        assert EXPECTED_DATE_STATE_COLUMNS <= date_columns
        assert REQUIRED_D4_DATE_COLUMNS <= date_columns

        def foreign_keys(table: str) -> set[tuple[str, str, str]]:
            return {
                (row[2], row[3], row[4])
                for row in connection.execute(f"PRAGMA foreign_key_list({table})")
            }

        assert ("sync_runs", "linked_sync_run_id", "run_id") in foreign_keys(
            "reconciliation_runs"
        )
        assert (
            "reconciliation_runs",
            "run_id",
            "run_id",
        ) in foreign_keys("reconciliation_events")
        assert (
            "reconciliation_runs",
            "reconciliation_run_id",
            "run_id",
        ) in foreign_keys("repair_candidates")
        assert (
            "reconciliation_runs",
            "reconciliation_run_id",
            "run_id",
        ) in foreign_keys("reconciliation_recheck_claims")
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_future_full_v1_shape_is_rejected_without_schema_mutation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "future.db"
    create_frozen_v1_database(database, schema_version=99)
    with sqlite3.connect(database) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        before_values = connection.execute(
            "SELECT * FROM sync_schema_metadata"
        ).fetchall()

    repository = make_repository(database, tmp_path)
    with pytest.raises(IncompatibleSchemaError, match="version 99"):
        repository.initialize()

    with sqlite3.connect(database) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        after_values = connection.execute(
            "SELECT * FROM sync_schema_metadata"
        ).fetchall()
        date_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(date_sync_state)")
        }
    assert after_schema == before_schema
    assert after_values == before_values
    assert not (EXPECTED_DATE_STATE_COLUMNS & date_columns)


def test_migration_failure_rolls_back_every_v2_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "rollback.db"
    create_frozen_v1_database(database)
    repository = make_repository(database, tmp_path)

    class FailingMigrationConnection(sqlite3.Connection):
        def execute(self, sql: str, parameters=(), /):  # type: ignore[no-untyped-def]
            if "CREATE TABLE IF NOT EXISTS reconciliation_events" in sql:
                raise sqlite3.OperationalError("injected migration failure")
            return super().execute(sql, parameters)

    def failing_connect() -> sqlite3.Connection:
        connection = sqlite3.connect(
            database,
            timeout=30.0,
            factory=FailingMigrationConnection,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    monkeypatch.setattr(repository, "_connect", failing_connect)
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        repository.initialize()

    with sqlite3.connect(database) as connection:
        version = connection.execute(
            "SELECT schema_version FROM sync_schema_metadata"
        ).fetchone()[0]
        date_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(date_sync_state)")
        }
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert connection.execute(
            "SELECT COUNT(*) FROM download_attempts"
        ).fetchone()[0] == 1
    assert version == 1
    assert not (EXPECTED_DATE_STATE_COLUMNS & date_columns)
    assert "reconciliation_runs" not in tables
    assert "reconciliation_events" not in tables


def test_attempt_evidence_counts_observations_and_distinct_runs(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path / "state.db", tmp_path)
    repository.initialize()

    first_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        first_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-08T01:00:00+00:00",
            http_status=200,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
        ),
    )
    repository.record_attempt(
        first_run,
        make_attempt(
            attempt_number=2,
            finished_at="2026-08-08T01:00:01+00:00",
            http_status=200,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
        ),
    )
    second_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        second_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-09T02:00:00+00:00",
            http_status=200,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
        ),
    )
    third_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        third_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-10T01:00:00+00:00",
            http_status=503,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.HTTP_FAILURE,
        ),
    )
    repository.record_attempt(
        third_run,
        make_attempt(
            attempt_number=2,
            finished_at="2026-08-10T01:00:01+00:00",
            http_status=200,
            classification="EQUITY_ROWS",
            status=DownloadStatus.TRADING_DATA,
            valid_rows=2,
            checksum="b" * 64,
            saved_path=tmp_path / "raw" / "market_2026-08-08.csv",
        ),
    )

    evidence_by_date = repository.get_attempt_evidence_for_range(
        MARKET_DATE, MARKET_DATE
    )
    evidence = evidence_by_date[MARKET_DATE]
    assert isinstance(evidence, AttemptEvidenceRecord)
    assert evidence.attempt_count == 5
    assert evidence.empty_observation_count == 3
    assert evidence.independent_empty_run_count == 2
    assert evidence.valid_observation_count == 1
    assert evidence.independent_valid_run_count == 1
    assert evidence.http_statuses == (200, 200, 200, 503, 200)
    assert evidence.response_classifications == (
        "EMPTY_MARKET_RESPONSE",
        "EMPTY_MARKET_RESPONSE",
        "EMPTY_MARKET_RESPONSE",
        "EMPTY_MARKET_RESPONSE",
        "EQUITY_ROWS",
    )
    assert evidence.empty_run_observations == (
        (first_run, "2026-08-08T01:00:00+00:00"),
        (second_run, "2026-08-09T02:00:00+00:00"),
    )
    assert evidence.latest_valid_checksum == "b" * 64
    assert evidence.latest_valid_relative_path == "raw/market_2026-08-08.csv"


def test_dry_run_audit_lifecycle_does_not_materialize_date_state(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path / "state.db", tmp_path)
    repository.initialize()
    run_id = repository.begin_reconciliation_run(
        policy_version=POLICY_VERSION,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.DRY_RUN,
        requested_date_count=1,
        worker_count=2,
        force_recheck=False,
        max_rechecks_per_date=1,
    )
    result = sample_date_result()
    audit = repository.finish_reconciliation_run(range_result(run_id, result))

    assert audit.run_id == run_id
    assert audit.mode is ReconciliationMode.DRY_RUN
    assert audit.status is ReconciliationRunStatus.COMPLETED
    assert audit.confirmed_non_trading_count == 1
    assert audit.status_transition_count == 1
    assert audit.complete
    assert repository.get_date_state(MARKET_DATE) is None
    assert repository.list_reconciliation_events(run_id) == ()


def test_reconciliation_event_records_bounded_structured_evidence(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path / "state.db", tmp_path)
    repository.initialize()
    run_id = repository.begin_reconciliation_run(
        policy_version=POLICY_VERSION,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
    )
    result = sample_date_result()
    repository.record_reconciliation_event(run_id, result)

    (event,) = repository.list_reconciliation_events(run_id)
    evidence = json.loads(event["evidence_summary"])
    assert event["market_date"] == MARKET_DATE
    assert event["previous_status"] == "EMPTY_UNRESOLVED"
    assert event["new_status"] == "CONFIRMED_NON_TRADING"
    assert event["action"] == "CONFIRM_NON_TRADING"
    assert event["policy_version"] == POLICY_VERSION
    assert evidence["independent_empty_run_count"] == 2
    assert evidence["reasons"] == [
        "two independent structurally empty observations"
    ]
    assert len(event["evidence_summary"]) < 8_000
    assert "response_body" not in event["evidence_summary"]

    oversized = replace(result, warnings=("x" * 8_000,))
    with pytest.raises(StateDatabaseError, match="evidence summary is too large"):
        repository.record_reconciliation_event(run_id, oversized)
    assert len(repository.list_reconciliation_events(run_id)) == 1

    with pytest.raises(sqlite3.IntegrityError):
        repository.record_reconciliation_event("unknown-run", result)


def test_reconciliation_event_compacts_long_attempt_history(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path / "state.db", tmp_path)
    repository.initialize()
    run_id = repository.begin_reconciliation_run(
        policy_version=POLICY_VERSION,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
    )
    result = sample_date_result()
    long_evidence = replace(
        result.evidence_summary,
        http_statuses=(503,) * 400,
        response_classifications=("EMPTY_MARKET_RESPONSE",) * 400,
    )

    repository.record_reconciliation_event(
        run_id,
        replace(
            result,
            evidence_summary=long_evidence,
            attempt_count=400,
            empty_observation_count=400,
        ),
    )

    (event,) = repository.list_reconciliation_events(run_id)
    evidence = json.loads(event["evidence_summary"])
    assert len(event["evidence_summary"]) < 8_000
    assert evidence["http_statuses_total_count"] == 400
    assert evidence["http_statuses_truncated_count"] == 350
    assert evidence["http_status_counts"] == {"503": 400}
    assert evidence["http_statuses"] == [503] * 50
    assert evidence["response_classifications_total_count"] == 400
    assert evidence["response_classification_counts"] == {
        "EMPTY_MARKET_RESPONSE": 400
    }


def test_confirmed_non_trading_transition_is_guarded_and_reversible_by_data(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path / "state.db", tmp_path)
    repository.initialize()
    empty_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        empty_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-08T01:00:00+00:00",
            http_status=200,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
        ),
    )
    second_empty_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        second_empty_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-09T02:00:00+00:00",
            http_status=200,
            classification="EMPTY_MARKET_RESPONSE",
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
        ),
    )
    before = repository.get_date_state(MARKET_DATE)
    assert before is not None

    changed = repository.confirm_non_trading(
        MARKET_DATE,
        policy_version=POLICY_VERSION,
        classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
        expected_record_updated_at=before.record_updated_at,
        canonical_path=tmp_path / "raw" / f"market_{MARKET_DATE}.csv",
    )
    confirmed = repository.get_date_state(MARKET_DATE)
    assert changed
    assert confirmed is not None
    assert confirmed.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
    assert confirmed.evidence_state is (
        SyncEvidenceState.REPEATED_EMPTY_WITH_WEEKEND_CALENDAR
    )
    assert confirmed.classification_policy_version == POLICY_VERSION
    assert confirmed.classification_basis == (
        WEEKEND_EMPTY_CLASSIFICATION_BASIS
    )
    assert datetime.fromisoformat(
        confirmed.classification_updated_at or ""
    ).tzinfo is not None

    with pytest.raises(StateDatabaseError, match="stale reconciliation plan"):
        repository.confirm_non_trading(
            MARKET_DATE,
            policy_version=POLICY_VERSION,
            classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
            expected_record_updated_at=before.record_updated_at,
            canonical_path=tmp_path / "raw" / f"market_{MARKET_DATE}.csv",
        )

    trading_run = repository.begin_sync_run(
        "reconcile", MARKET_DATE, MARKET_DATE, 1, 1
    )
    repository.record_attempt(
        trading_run,
        make_attempt(
            attempt_number=1,
            finished_at="2026-08-11T01:00:00+00:00",
            http_status=200,
            classification="EQUITY_ROWS",
            status=DownloadStatus.TRADING_DATA,
            valid_rows=2,
            checksum="c" * 64,
            saved_path=tmp_path / "raw" / "market_2026-08-08.csv",
        ),
    )
    verified = repository.get_date_state(MARKET_DATE)
    assert verified is not None
    assert verified.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    with pytest.raises(
        StateDatabaseError,
        match="historically verified date",
    ):
        repository.confirm_non_trading(
            MARKET_DATE,
            policy_version=POLICY_VERSION,
            classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
            expected_record_updated_at=verified.record_updated_at,
            canonical_path=tmp_path / "raw" / f"market_{MARKET_DATE}.csv",
        )
