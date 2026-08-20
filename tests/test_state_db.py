from __future__ import annotations

import sqlite3
from datetime import date, datetime
from pathlib import Path

import pytest

from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import (
    DownloadAttemptEvent,
    DownloadResult,
    DownloadStatus,
    PersistentSyncStatus,
    SyncEvidenceState,
    SyncRunStatus,
)
from psx_data_sync.state_db import (
    EXPECTED_INDEXES,
    EXPECTED_TABLES,
    SCHEMA_VERSION,
    IncompatibleSchemaError,
    StateRepository,
    resolve_status_transition,
)
from psx_data_sync.validator import validate_rows


STARTED = "2026-08-19T12:00:00+00:00"
FINISHED = "2026-08-19T12:00:00.100000+00:00"


def make_repository(tmp_path: Path) -> StateRepository:
    repository = StateRepository(
        tmp_path / "state" / "psx_sync.db",
        project_root=tmp_path,
    )
    repository.initialize()
    return repository


def make_valid_csv(
    output_dir: Path,
    market_date: date,
    fixture_bytes,
    *,
    row_limit: int | None = None,
):
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    if row_limit is not None:
        rows = rows[:row_limit]
    return save_canonical_csv(rows, market_date, output_dir)


def make_attempt(
    market_date: str,
    attempt_number: int,
    status: DownloadStatus,
    *,
    http_status: int | None = None,
    response_bytes: int = 0,
    parsed_rows: int = 0,
    valid_rows: int = 0,
    rejected_rows: int = 0,
    checksum: str | None = None,
    saved_path: Path | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> DownloadAttemptEvent:
    return DownloadAttemptEvent(
        requested_date=market_date,
        attempt_number=attempt_number,
        started_at=STARTED,
        finished_at=FINISHED,
        duration_ms=100,
        http_status=http_status,
        response_bytes=response_bytes,
        response_classification=(
            "EQUITY_ROWS" if status is DownloadStatus.TRADING_DATA else None
        ),
        final_status=status,
        retryable=status is not DownloadStatus.TRADING_DATA,
        error_type=error_type,
        error_message=error_message,
        parsed_row_count=parsed_rows,
        valid_row_count=valid_rows,
        rejected_row_count=rejected_rows,
        checksum=checksum,
        saved_path=saved_path,
        worker_identifier="worker-2",
    )


def begin_run(repository: StateRepository, market_date: str = "2026-08-05") -> str:
    return repository.begin_sync_run(
        "fetch", market_date, market_date, requested_date_count=1, worker_count=1
    )


def test_schema_initialization_is_versioned_idempotent_and_safe(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    repository.initialize()

    tables, indexes = repository.schema_objects()
    settings = repository.database_settings()

    assert repository.verify_schema() == SCHEMA_VERSION == 3
    assert EXPECTED_TABLES <= tables
    assert EXPECTED_INDEXES <= indexes
    assert settings["foreign_keys"] == 1
    assert settings["journal_mode"] == "wal"
    assert settings["synchronous"] == 2


def test_incompatible_future_schema_fails_without_mutation(tmp_path: Path) -> None:
    database = tmp_path / "future.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE sync_schema_metadata (
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                application_version TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO sync_schema_metadata VALUES (1, 99, 'x', 'x', 'future')"
        )

    repository = StateRepository(database, project_root=tmp_path)
    with pytest.raises(IncompatibleSchemaError, match="version 99"):
        repository.initialize()

    with sqlite3.connect(database) as connection:
        version = connection.execute(
            "SELECT schema_version FROM sync_schema_metadata"
        ).fetchone()[0]
        table_count = connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
    assert version == 99
    assert table_count == 1


def test_canonical_transition_policy() -> None:
    assert resolve_status_transition(
        PersistentSyncStatus.EMPTY_UNRESOLVED,
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
    ) is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert resolve_status_transition(
        PersistentSyncStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
    ) is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert resolve_status_transition(
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
        PersistentSyncStatus.ALREADY_PRESENT_VERIFIED,
    ) is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert resolve_status_transition(
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
        PersistentSyncStatus.TEMPORARY_FAILURE,
    ) is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert resolve_status_transition(
        PersistentSyncStatus.VERIFIED_TRADING_DATA,
        PersistentSyncStatus.FILE_CORRUPT,
    ) is PersistentSyncStatus.FILE_CORRUPT


def test_attempt_history_and_lifetime_counters_are_transactional(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    run_id = begin_run(repository)
    repository.record_attempt(
        run_id,
        make_attempt(
            "2026-08-05",
            1,
            DownloadStatus.HTTP_FAILURE,
            http_status=503,
            response_bytes=21,
            error_type="HTTP",
            error_message="  upstream   unavailable  ",
        ),
    )
    repository.record_attempt(
        run_id,
        make_attempt(
            "2026-08-05",
            2,
            DownloadStatus.TRADING_DATA,
            http_status=200,
            response_bytes=1000,
            parsed_rows=3,
            valid_rows=3,
            checksum="abc",
            saved_path=tmp_path / "raw" / "market_2026-08-05.csv",
        ),
    )

    state = repository.get_date_state("2026-08-05")
    attempts = tuple(reversed(repository.get_recent_attempts("2026-08-05")))

    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.evidence_state is SyncEvidenceState.NETWORK_VALIDATED_CSV
    assert state.attempt_count == 2
    assert state.successful_attempt_count == 1
    assert state.csv_checksum_sha256 == "abc"
    assert state.csv_relative_path == "raw/market_2026-08-05.csv"
    assert state.last_success_at == FINISHED
    assert state.first_attempt_at == STARTED
    assert datetime.fromisoformat(state.record_updated_at).utcoffset() is not None
    assert [item.attempt_number for item in attempts] == [1, 2]
    assert attempts[0].http_status == 503
    assert attempts[0].error_message == "upstream unavailable"
    assert attempts[1].response_classification == "EQUITY_ROWS"
    assert not hasattr(attempts[0], "response_body")

    with pytest.raises(sqlite3.IntegrityError):
        repository.record_attempt(
            run_id,
            make_attempt("2026-08-05", 2, DownloadStatus.TRADING_DATA),
        )
    assert repository.get_date_state("2026-08-05").attempt_count == 2


def test_verified_success_metadata_survives_later_transient_attempt(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    first_run = begin_run(repository)
    artifact = tmp_path / "raw" / "market_2026-08-05.csv"
    repository.record_attempt(
        first_run,
        make_attempt(
            "2026-08-05",
            1,
            DownloadStatus.TRADING_DATA,
            http_status=200,
            parsed_rows=3,
            valid_rows=3,
            checksum="verified",
            saved_path=artifact,
        ),
    )
    before = repository.get_date_state("2026-08-05")

    second_run = begin_run(repository)
    repository.record_attempt(
        second_run,
        make_attempt(
            "2026-08-05",
            1,
            DownloadStatus.TEMPORARY_FAILURE,
            error_type="TIMEOUT",
            error_message="timed out",
        ),
    )
    after = repository.get_date_state("2026-08-05")

    assert before is not None and after is not None
    assert after.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert after.attempt_count == 2
    assert after.valid_row_count == 3
    assert after.csv_checksum_sha256 == "verified"
    assert after.csv_relative_path == "raw/market_2026-08-05.csv"
    assert after.last_success_at == before.last_success_at
    assert after.last_error_type == "TIMEOUT"


def test_sync_runs_keep_separate_summaries_and_interruption(tmp_path: Path) -> None:
    repository = make_repository(tmp_path)
    completed_id = repository.begin_sync_run(
        "fetch-range", "2026-08-04", "2026-08-05", 2, 4
    )
    repository.record_attempt(
        completed_id,
        make_attempt(
            "2026-08-04",
            1,
            DownloadStatus.TRADING_DATA,
            response_bytes=500,
            valid_rows=3,
        ),
    )
    repository.record_download_result(
        completed_id,
        DownloadResult(
            "2026-08-04",
            DownloadStatus.TRADING_DATA,
            attempts=1,
            valid_row_count=3,
            cumulative_response_bytes=500,
        ),
    )
    repository.record_download_result(
        completed_id,
        DownloadResult(
            "2026-08-05",
            DownloadStatus.NON_TRADING_OR_EMPTY,
            attempts=1,
        ),
    )
    completed = repository.finish_sync_run(completed_id, duration_ms=432.1)

    interrupted_id = repository.begin_sync_run(
        "fetch-range", "2026-08-04", "2026-08-05", 2, 2
    )
    interrupted = repository.finish_sync_run(
        interrupted_id, interrupted=True, duration_ms=10
    )

    assert completed.status is SyncRunStatus.COMPLETED_WITH_UNRESOLVED
    assert completed.requested_date_count == 2
    assert completed.worker_count == 4
    assert completed.completed_count == 2
    assert completed.network_fetch_count == 1
    assert completed.success_count == 1
    assert completed.unresolved_count == 1
    assert completed.total_attempts == 1
    assert completed.total_response_bytes == 500
    assert completed.duration_ms == 432.1
    assert interrupted.run_id != completed.run_id
    assert interrupted.status is SyncRunStatus.INTERRUPTED
    assert interrupted.interrupted


def test_bootstrap_indexes_valid_files_flags_invalid_and_is_idempotent(
    tmp_path: Path, fixture_bytes
) -> None:
    output_dir = tmp_path / "data" / "raw"
    valid = make_valid_csv(output_dir, date(2026, 8, 5), fixture_bytes)
    invalid = output_dir / "market_2026-08-04.csv"
    invalid.write_text("corrupt\n", encoding="utf-8")
    repository = make_repository(tmp_path)

    first = repository.bootstrap_local_files(output_dir)
    second = repository.bootstrap_local_files(output_dir)
    state = repository.get_date_state("2026-08-05")
    corrupt = repository.get_date_state("2026-08-04")

    assert first.discovered_files == 2
    assert first.indexed_files == 1
    assert first.invalid_files == 1
    assert second.indexed_files == 0
    assert second.unchanged_files == 1
    assert second.invalid_files == 1
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.valid_row_count == 3
    assert state.csv_checksum_sha256 == valid.checksum
    assert state.csv_relative_path == "data/raw/market_2026-08-05.csv"
    assert not Path(state.csv_relative_path).is_absolute()
    assert state.attempt_count == 0
    assert repository.get_recent_attempts("2026-08-05") == ()
    assert corrupt is not None
    assert corrupt.status is PersistentSyncStatus.FILE_CORRUPT
    assert corrupt.csv_relative_path == "data/raw/market_2026-08-04.csv"


def test_matching_verified_file_and_unindexed_valid_file_skip_network(
    tmp_path: Path, fixture_bytes
) -> None:
    output_dir = tmp_path / "raw"
    first = date(2026, 8, 4)
    second = date(2026, 8, 5)
    make_valid_csv(output_dir, first, fixture_bytes)
    make_valid_csv(output_dir, second, fixture_bytes)
    repository = make_repository(tmp_path)
    repository.bootstrap_local_files(output_dir)

    matching = repository.prepare_fetch(first, output_dir)
    unindexed_repository = StateRepository(
        tmp_path / "other.db", project_root=tmp_path
    )
    unindexed_repository.initialize()
    unindexed = unindexed_repository.prepare_fetch(second, output_dir)

    assert matching is not None and matching.locally_skipped
    assert matching.status is DownloadStatus.ALREADY_PRESENT
    assert matching.attempts == 0
    assert unindexed is not None and unindexed.locally_skipped
    assert unindexed_repository.get_date_state(
        second
    ).status is PersistentSyncStatus.VERIFIED_TRADING_DATA


def test_local_skip_does_not_claim_a_different_configured_endpoint(
    tmp_path: Path, fixture_bytes
) -> None:
    output_dir = tmp_path / "raw"
    market_date = date(2026, 8, 5)
    make_valid_csv(output_dir, market_date, fixture_bytes)
    original = make_repository(tmp_path)
    original.bootstrap_local_files(output_dir)
    original_source = original.get_date_state(market_date).source_endpoint

    alternate = StateRepository(
        original.database_path,
        project_root=tmp_path,
        source_endpoint="http://127.0.0.1:1/not-contacted",
    )
    alternate.initialize()
    result = alternate.prepare_fetch(market_date, output_dir)
    assert result is not None and result.attempts == 0
    run_id = begin_run(alternate)
    alternate.record_download_result(run_id, result)

    assert alternate.get_date_state(market_date).source_endpoint == original_source


def test_missing_modified_and_invalid_verified_files_are_detected(
    tmp_path: Path, fixture_bytes
) -> None:
    output_dir = tmp_path / "raw"
    repository = make_repository(tmp_path)

    missing_day = date(2026, 8, 3)
    missing = make_valid_csv(output_dir, missing_day, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    missing.path.unlink()
    missing_outcome = repository.prepare_fetch(missing_day, output_dir)
    missing_state = repository.get_date_state(missing_day)
    assert missing_outcome is not None
    assert missing_outcome.status is DownloadStatus.REPAIR_REQUIRED
    assert missing_outcome.attempts == 0
    assert missing_state.status is PersistentSyncStatus.FILE_MISSING
    assert missing_state.csv_checksum_sha256 == missing.checksum

    conflict_day = date(2026, 8, 4)
    original = make_valid_csv(output_dir, conflict_day, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    alternate_dir = tmp_path / "alternate"
    alternate = make_valid_csv(
        alternate_dir, conflict_day, fixture_bytes, row_limit=1
    )
    original.path.write_bytes(alternate.path.read_bytes())
    conflict = repository.prepare_fetch(conflict_day, output_dir)
    conflict_state = repository.get_date_state(conflict_day)
    assert conflict is not None
    assert conflict.status is DownloadStatus.REPAIR_REQUIRED
    assert conflict_state.status is PersistentSyncStatus.FILE_CONFLICT
    assert conflict_state.csv_checksum_sha256 == original.checksum
    assert original.path.read_bytes() == alternate.path.read_bytes()

    corrupt_day = date(2026, 8, 5)
    corrupt = make_valid_csv(output_dir, corrupt_day, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    corrupt.path.write_text("not,csv\n", encoding="utf-8")
    invalid = repository.prepare_fetch(corrupt_day, output_dir)
    repeated_invalid = repository.prepare_fetch(corrupt_day, output_dir)
    corrupt_state = repository.get_date_state(corrupt_day)
    assert invalid is not None
    assert invalid.status is DownloadStatus.REPAIR_REQUIRED
    assert repeated_invalid is not None
    assert repeated_invalid.status is DownloadStatus.REPAIR_REQUIRED
    assert corrupt_state.status is PersistentSyncStatus.FILE_CORRUPT
    assert corrupt_state.csv_checksum_sha256 == corrupt.checksum
    assert corrupt.path.read_text(encoding="utf-8") == "not,csv\n"


@pytest.mark.parametrize(
    "persistent_status",
    [
        PersistentSyncStatus.EMPTY_UNRESOLVED,
        PersistentSyncStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.HTTP_FAILURE,
        PersistentSyncStatus.PARSE_FAILURE,
        PersistentSyncStatus.VALIDATION_FAILURE,
    ],
)
def test_unresolved_and_failure_states_remain_network_eligible(
    tmp_path: Path, persistent_status: PersistentSyncStatus
) -> None:
    repository = make_repository(tmp_path)
    download_status = {
        PersistentSyncStatus.EMPTY_UNRESOLVED: DownloadStatus.EMPTY_MARKET_RESPONSE,
        PersistentSyncStatus.TEMPORARY_FAILURE: DownloadStatus.TEMPORARY_FAILURE,
        PersistentSyncStatus.HTTP_FAILURE: DownloadStatus.HTTP_FAILURE,
        PersistentSyncStatus.PARSE_FAILURE: DownloadStatus.PARSE_FAILURE,
        PersistentSyncStatus.VALIDATION_FAILURE: DownloadStatus.VALIDATION_FAILURE,
    }[persistent_status]
    run_id = begin_run(repository)
    repository.record_attempt(
        run_id, make_attempt("2026-08-05", 1, download_status)
    )

    assert repository.prepare_fetch(date(2026, 8, 5), tmp_path / "raw") is None


def test_summary_and_status_query_are_date_filtered(tmp_path: Path) -> None:
    repository = make_repository(tmp_path)
    run_id = repository.begin_sync_run(
        "fetch-range", "2026-08-01", "2026-08-03", 3, 2
    )
    repository.record_attempt(
        run_id,
        make_attempt("2026-08-01", 1, DownloadStatus.EMPTY_MARKET_RESPONSE),
    )
    repository.record_attempt(
        run_id,
        make_attempt("2026-08-02", 1, DownloadStatus.HTTP_FAILURE),
    )

    summary = repository.summarize_range(
        start_date="2026-08-02", end_date="2026-08-03"
    )
    failures = repository.list_dates_by_status(
        [PersistentSyncStatus.HTTP_FAILURE]
    )

    assert summary.tracked_dates == 1
    assert summary.earliest_tracked == "2026-08-02"
    assert summary.counts_by_status[PersistentSyncStatus.HTTP_FAILURE] == 1
    assert [item.market_date for item in failures] == ["2026-08-02"]
