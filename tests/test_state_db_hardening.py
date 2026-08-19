from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

from psx_data_sync.config import Settings
from psx_data_sync.downloader import SingleDateDownloader
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import (
    ChecksumState,
    DateReconciliationResult,
    DownloadAttemptEvent,
    DownloadResult,
    DownloadStatus,
    FileHealthState,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationEvidenceSummary,
    ReconciliationMode,
    RECONCILIATION_POLICY_VERSION,
    SyncEvidenceState,
    WEEKEND_EMPTY_CLASSIFICATION_BASIS,
)
from psx_data_sync.state_db import (
    AsyncStateRepository,
    StateDatabaseError,
    StateRepository,
)
from psx_data_sync.synchronizer import ConcurrentRangeDownloader
from psx_data_sync.validator import validate_rows


MARKET_DAY = date(2026, 8, 8)
MARKET_DATE = MARKET_DAY.isoformat()


def make_repository(tmp_path: Path) -> StateRepository:
    repository = StateRepository(tmp_path / "state.db", project_root=tmp_path)
    repository.initialize()
    return repository


def make_valid_csv(
    output_dir: Path,
    market_day: date,
    fixture_bytes,
    *,
    row_limit: int | None = None,
):
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    if row_limit is not None:
        rows = rows[:row_limit]
    return save_canonical_csv(rows, market_day, output_dir)


class NoNetworkSyncClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, requested_date: date):
        self.calls.append(requested_date.isoformat())
        raise AssertionError("ordinary repair preflight reached HTTP")


class NoNetworkAsyncClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, requested_date: date):
        self.calls.append(requested_date.isoformat())
        raise AssertionError("ordinary range repair preflight reached HTTP")


def begin_sync_run(repository: StateRepository, market_date: str = MARKET_DATE) -> str:
    return repository.begin_sync_run(
        "reconcile", market_date, market_date, 1, 1
    )


def make_attempt(
    *,
    market_date: str = MARKET_DATE,
    finished_at: str,
    status: DownloadStatus,
    http_status: int | None = None,
    classification: str | None = None,
    valid_rows: int = 0,
    error_type: str | None = None,
    checksum: str | None = None,
    saved_path: Path | None = None,
) -> DownloadAttemptEvent:
    return DownloadAttemptEvent(
        requested_date=market_date,
        attempt_number=1,
        started_at=finished_at,
        finished_at=finished_at,
        duration_ms=1.0,
        http_status=http_status,
        response_bytes=100,
        response_classification=classification,
        final_status=status,
        retryable=status is not DownloadStatus.TRADING_DATA,
        error_type=error_type,
        error_message=error_type,
        parsed_row_count=valid_rows,
        valid_row_count=valid_rows,
        checksum=checksum,
        saved_path=saved_path,
        worker_identifier="state-hardening-test",
    )


def add_independent_empty_observations(repository: StateRepository) -> None:
    observations = (
        "2026-08-08T01:00:00+00:00",
        "2026-08-09T02:00:00+00:00",
    )
    for finished_at in observations:
        run_id = begin_sync_run(repository)
        repository.record_attempt(
            run_id,
            make_attempt(
                finished_at=finished_at,
                status=DownloadStatus.EMPTY_MARKET_RESPONSE,
                http_status=200,
                classification="EMPTY_MARKET_RESPONSE",
            ),
        )


def confirm_weekend(repository: StateRepository, tmp_path: Path) -> None:
    add_independent_empty_observations(repository)
    current = repository.get_date_state(MARKET_DATE)
    assert current is not None
    repository.confirm_non_trading(
        MARKET_DATE,
        policy_version=RECONCILIATION_POLICY_VERSION,
        classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
        expected_record_updated_at=current.record_updated_at,
        canonical_path=tmp_path / "raw" / f"market_{MARKET_DATE}.csv",
    )


def make_decision(
    *,
    previous_status: PersistentSyncStatus,
    reconciled_status: PersistentSyncStatus,
    action: ReconciliationAction,
    expected_path: Path,
) -> DateReconciliationResult:
    return DateReconciliationResult(
        market_date=MARKET_DATE,
        previous_status=previous_status,
        reconciled_status=reconciled_status,
        policy_version=RECONCILIATION_POLICY_VERSION,
        evidence_classification="LOCAL_ARTIFACT",
        action_required=action,
        network_recheck_required=False,
        recheck_eligible_now=False,
        local_repair_required=False,
        evidence_summary=ReconciliationEvidenceSummary(
            weekday="Saturday",
            calendar_weekend=True,
            calendar_support="WEEKEND",
            persistent_evidence=None,
            http_statuses=(),
            response_classifications=(),
            independent_empty_run_count=0,
            independent_valid_run_count=0,
            adjacent_previous_verified=False,
            adjacent_next_verified=False,
            expected_csv_path=str(expected_path),
            expected_checksum=None,
            observed_checksum=None,
        ),
        attempt_count=0,
        empty_observation_count=0,
        valid_observation_count=0,
        file_state=FileHealthState.UNTRACKED_VALID,
        checksum_state=ChecksumState.UNTRACKED,
        reasons=("test decision",),
    )


def test_unverified_corrupt_bytes_are_not_persisted_as_canonical_identity(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "raw"
    output_dir.mkdir()
    artifact = output_dir / f"market_{MARKET_DATE}.csv"
    artifact.write_bytes(b"not,a,canonical,csv\n")
    repository = make_repository(tmp_path)

    result = repository.bootstrap_local_files(output_dir)
    state = repository.get_date_state(MARKET_DATE)

    assert result.invalid_files == 1
    assert result.files[0].checksum is not None
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_CORRUPT
    assert state.csv_checksum_sha256 is None
    assert state.csv_relative_path == f"raw/market_{MARKET_DATE}.csv"


def test_checksum_alone_does_not_block_confirmation_and_metadata_is_cleared(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    add_independent_empty_observations(repository)
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            """
            UPDATE date_sync_state SET
                parsed_row_count = 7, valid_row_count = 3,
                rejected_row_count = 4, csv_checksum_sha256 = ?,
                csv_relative_path = ?
            WHERE market_date = ?
            """,
            (
                "f" * 64,
                f"raw/market_{MARKET_DATE}.csv",
                MARKET_DATE,
            ),
        )
    current = repository.get_date_state(MARKET_DATE)
    assert current is not None and current.last_verified_at is None

    repository.confirm_non_trading(
        MARKET_DATE,
        policy_version=RECONCILIATION_POLICY_VERSION,
        classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
        expected_record_updated_at=current.record_updated_at,
        canonical_path=tmp_path / "raw" / f"market_{MARKET_DATE}.csv",
    )
    confirmed = repository.get_date_state(MARKET_DATE)

    assert confirmed is not None
    assert confirmed.status is PersistentSyncStatus.CONFIRMED_NON_TRADING
    assert confirmed.parsed_row_count == 0
    assert confirmed.valid_row_count == 0
    assert confirmed.rejected_row_count == 0
    assert confirmed.csv_checksum_sha256 is None
    assert confirmed.csv_relative_path is None


@pytest.mark.parametrize("staged", [False, True])
def test_valid_rows_with_save_failure_break_confirmed_conclusion(
    tmp_path: Path,
    staged: bool,
) -> None:
    repository = make_repository(tmp_path)
    confirm_weekend(repository, tmp_path)
    run_id = begin_sync_run(repository)
    event = make_attempt(
        finished_at="2026-08-10T03:00:00+00:00",
        status=DownloadStatus.SAVE_FAILURE,
        http_status=200,
        classification="EQUITY_ROWS",
        valid_rows=2,
        error_type="SAVE_FAILURE",
    )

    recorder = (
        repository.record_staged_attempt if staged else repository.record_attempt
    )
    recorder(run_id, event)
    state = repository.get_date_state(MARKET_DATE)

    assert state is not None
    assert state.status is PersistentSyncStatus.TEMPORARY_FAILURE
    assert state.evidence_state is SyncEvidenceState.NETWORK_OBSERVATION
    assert state.valid_row_count == 2
    assert state.classification_policy_version is None
    assert state.classification_basis is None
    assert state.classification_updated_at is None


def test_prepare_fetch_rejects_stale_artifact_snapshot_without_an_event(
    tmp_path: Path,
    fixture_bytes,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    saved = make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)
    assert saved.checksum is not None
    run_id = repository.begin_reconciliation_run(
        policy_version=RECONCILIATION_POLICY_VERSION,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
    )
    decision = make_decision(
        previous_status=PersistentSyncStatus.NEVER_ATTEMPTED,
        reconciled_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
        action=ReconciliationAction.LOCAL_REINDEX,
        expected_path=saved.path,
    )
    saved.path.write_bytes(b"changed-after-planning\n")

    with pytest.raises(StateDatabaseError, match="validity changed after planning"):
        repository.prepare_fetch(
            MARKET_DAY,
            output_dir,
            expected_state_exists=False,
            reconciliation_run_id=run_id,
            reconciliation_decision=decision,
            expected_artifact_path=saved.path,
            expected_artifact_exists=True,
            expected_artifact_valid=True,
            expected_observed_checksum=saved.checksum,
        )

    assert repository.get_date_state(MARKET_DATE) is None
    assert repository.list_reconciliation_events(run_id) == ()


def test_untrusted_conflict_is_preserved_but_exact_trusted_identity_self_heals(
    tmp_path: Path,
    fixture_bytes,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    repository.mark_artifact_issue(
        MARKET_DATE,
        PersistentSyncStatus.FILE_CONFLICT,
        error_type="CONFLICT",
        error_message="identity is untrusted",
    )
    untrusted_before = repository.get_date_state(MARKET_DATE)
    make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)

    blocked = repository.prepare_fetch(MARKET_DAY, output_dir)
    untrusted_after = repository.get_date_state(MARKET_DATE)

    assert blocked is not None and blocked.status is DownloadStatus.FILE_CONFLICT
    assert untrusted_before == untrusted_after
    assert untrusted_after is not None
    assert untrusted_after.last_verified_at is None
    assert untrusted_after.csv_checksum_sha256 is None

    bootstrap = repository.bootstrap_local_files(output_dir)
    (untrusted_outcome,) = bootstrap.files
    untrusted_after_bootstrap = repository.get_date_state(MARKET_DATE)
    assert untrusted_outcome.status is PersistentSyncStatus.FILE_CONFLICT
    assert not untrusted_outcome.changed
    assert untrusted_after_bootstrap == untrusted_after

    trusted_day = date(2026, 8, 9)
    trusted_saved = make_valid_csv(output_dir, trusted_day, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    repository.mark_artifact_issue(
        trusted_day.isoformat(),
        PersistentSyncStatus.FILE_CONFLICT,
        error_type="CONFLICT",
        error_message="requires exact identity",
    )

    healed = repository.prepare_fetch(trusted_day, output_dir)
    trusted_state = repository.get_date_state(trusted_day.isoformat())

    assert healed is not None and healed.status is DownloadStatus.ALREADY_PRESENT
    assert trusted_state is not None
    assert trusted_state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert trusted_state.csv_checksum_sha256 == trusted_saved.checksum


def test_mismatched_reconciliation_event_rolls_back_state_mutation(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    expected_path = tmp_path / "raw" / f"market_{MARKET_DATE}.csv"
    run_id = repository.begin_reconciliation_run(
        policy_version=RECONCILIATION_POLICY_VERSION,
        start_date=MARKET_DATE,
        end_date=MARKET_DATE,
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
    )
    misleading = make_decision(
        previous_status=PersistentSyncStatus.NEVER_ATTEMPTED,
        reconciled_status=PersistentSyncStatus.VERIFIED_TRADING_DATA,
        action=ReconciliationAction.LOCAL_REINDEX,
        expected_path=expected_path,
    )

    with pytest.raises(StateDatabaseError, match="status does not match"):
        repository.mark_artifact_issue(
            MARKET_DATE,
            PersistentSyncStatus.FILE_CORRUPT,
            error_type="FILE_CORRUPT",
            error_message="corrupt",
            expected_state_exists=False,
            reconciliation_run_id=run_id,
            reconciliation_decision=misleading,
        )

    assert repository.get_date_state(MARKET_DATE) is None
    assert repository.list_reconciliation_events(run_id) == ()


def test_schema_verification_rejects_missing_reconciliation_column(
    tmp_path: Path,
) -> None:
    repository = make_repository(tmp_path)
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            "ALTER TABLE reconciliation_events DROP COLUMN evidence_classification"
        )

    with pytest.raises(
        StateDatabaseError,
        match="reconciliation_events columns: evidence_classification",
    ):
        repository.verify_schema()


def test_single_fetch_routes_missing_trusted_artifact_to_staged_repair(
    tmp_path: Path,
    fixture_bytes,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    saved = make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    verified = repository.get_date_state(MARKET_DATE)
    assert verified is not None
    saved.path.unlink()
    run_id = begin_sync_run(repository)
    client = NoNetworkSyncClient()
    settings = Settings(
        raw_output_dir=output_dir,
        retry_attempts=1,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
    )
    downloader = SingleDateDownloader(
        settings,
        client,
        preflight=lambda day: repository.prepare_fetch(day, output_dir),
        attempt_observer=lambda event: repository.record_attempt(run_id, event),
    )

    result = downloader.download(MARKET_DATE)
    repository.record_download_result(run_id, result)
    run = repository.finish_sync_run(run_id)
    state = repository.get_date_state(MARKET_DATE)

    assert result.status is DownloadStatus.REPAIR_REQUIRED
    assert result.attempts == 0
    assert result.locally_skipped
    assert client.calls == []
    assert "reconcile" in (result.error or "")
    assert "--apply" in (result.error or "")
    assert not saved.path.exists()
    assert repository.get_recent_attempts(MARKET_DATE) == ()
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_MISSING
    assert state.csv_checksum_sha256 == saved.checksum
    assert state.valid_row_count == verified.valid_row_count
    assert state.csv_relative_path == verified.csv_relative_path
    assert run.network_fetch_count == 0
    assert run.local_skip_count == 1
    assert run.failure_count == 1


@pytest.mark.asyncio
async def test_range_fetch_routes_trusted_artifact_issues_without_http(
    tmp_path: Path,
    fixture_bytes,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    days = (date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12))
    saved_by_date = {
        day.isoformat(): make_valid_csv(output_dir, day, fixture_bytes)
        for day in days
    }
    repository.bootstrap_local_files(output_dir)
    original_states = {
        day.isoformat(): repository.get_date_state(day) for day in days
    }
    saved_by_date[days[0].isoformat()].path.unlink()
    saved_by_date[days[1].isoformat()].path.write_bytes(b"corrupt\n")
    alternate = make_valid_csv(
        tmp_path / "alternate",
        days[2],
        fixture_bytes,
        row_limit=1,
    )
    saved_by_date[days[2].isoformat()].path.write_bytes(
        alternate.path.read_bytes()
    )
    run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), len(days), 2
    )
    async_repository = AsyncStateRepository(repository)
    client = NoNetworkAsyncClient()
    settings = Settings(
        raw_output_dir=output_dir,
        retry_attempts=1,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
    )
    downloader = ConcurrentRangeDownloader(
        settings,
        client,
        workers=2,
        preflight=lambda day: async_repository.prepare_fetch(day, output_dir),
        attempt_observer=lambda event: async_repository.record_attempt(
            run_id, event
        ),
        result_observer=lambda result: async_repository.record_download_result(
            run_id, result
        ),
    )

    result = await downloader.download_dates(days)
    run = repository.finish_sync_run(run_id)

    assert client.calls == []
    assert result.network_fetched_dates == 0
    assert result.locally_skipped_dates == 3
    assert result.failed_dates == tuple(day.isoformat() for day in days)
    assert all(
        item.status is DownloadStatus.REPAIR_REQUIRED
        and item.attempts == 0
        and "reconcile" in (item.error or "")
        and "--apply" in (item.error or "")
        for item in result.results
    )
    assert run.network_fetch_count == 0
    assert run.local_skip_count == 3
    assert run.failure_count == 3
    expected_statuses = (
        PersistentSyncStatus.FILE_MISSING,
        PersistentSyncStatus.FILE_CORRUPT,
        PersistentSyncStatus.FILE_CONFLICT,
    )
    for day, expected_status in zip(days, expected_statuses, strict=True):
        date_text = day.isoformat()
        original = original_states[date_text]
        state = repository.get_date_state(date_text)
        assert original is not None and state is not None
        assert state.status is expected_status
        assert state.csv_checksum_sha256 == original.csv_checksum_sha256
        assert state.csv_relative_path == original.csv_relative_path
        assert state.parsed_row_count == original.parsed_row_count
        assert state.valid_row_count == original.valid_row_count
        assert state.rejected_row_count == original.rejected_row_count
        assert repository.get_recent_attempts(date_text) == ()


def test_staged_repair_preflight_preview_never_reindexes_state(
    tmp_path: Path,
    fixture_bytes,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    saved = make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)
    canonical_bytes = saved.path.read_bytes()
    repository.bootstrap_local_files(output_dir)
    saved.path.unlink()
    ordinary = repository.prepare_fetch(MARKET_DAY, output_dir)
    assert ordinary is not None
    assert ordinary.status is DownloadStatus.REPAIR_REQUIRED
    missing = repository.get_date_state(MARKET_DATE)
    assert missing is not None
    saved.path.write_bytes(canonical_bytes)

    preview = repository.prepare_fetch(
        MARKET_DAY,
        output_dir,
        allow_staged_repair=True,
        mutate_state=False,
    )

    assert preview is not None
    assert preview.status is DownloadStatus.ALREADY_PRESENT
    assert preview.locally_skipped
    assert repository.get_date_state(MARKET_DATE) == missing


@pytest.mark.parametrize(
    "failure_status",
    [
        DownloadStatus.HTTP_FAILURE,
        DownloadStatus.SAVE_FAILURE,
        DownloadStatus.FILE_CONFLICT,
        DownloadStatus.EXISTING_FILE_INVALID,
    ],
)
def test_transient_attempt_and_result_preserve_missing_verified_history(
    tmp_path: Path,
    fixture_bytes,
    failure_status: DownloadStatus,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    saved = make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)
    original_bytes = saved.path.read_bytes()
    repository.bootstrap_local_files(output_dir)
    verified = repository.get_date_state(MARKET_DATE)
    assert verified is not None
    saved.path.unlink()

    preflight = repository.prepare_fetch(MARKET_DAY, output_dir)
    assert preflight is not None
    assert preflight.status is DownloadStatus.REPAIR_REQUIRED
    missing = repository.get_date_state(MARKET_DATE)
    assert missing is not None
    assert missing.status is PersistentSyncStatus.FILE_MISSING
    run_id = begin_sync_run(repository)
    observed_rows = (
        0 if failure_status is DownloadStatus.HTTP_FAILURE else 99
    )
    repository.record_attempt(
        run_id,
        make_attempt(
            finished_at="2026-08-10T03:00:00+00:00",
            status=failure_status,
            http_status=(
                503 if failure_status is DownloadStatus.HTTP_FAILURE else 200
            ),
            classification="EQUITY_ROWS" if observed_rows else None,
            valid_rows=observed_rows,
            error_type=failure_status.value,
            checksum="untrusted-failure-checksum",
            saved_path=tmp_path / "staging" / "candidate.csv",
        ),
    )
    repository.record_download_result(
        run_id,
        DownloadResult(
            requested_date=MARKET_DATE,
            status=failure_status,
            http_status=(
                503 if failure_status is DownloadStatus.HTTP_FAILURE else 200
            ),
            attempts=1,
            parsed_row_count=observed_rows,
            valid_row_count=observed_rows,
            elapsed_ms=10.0,
            checksum="untrusted-result-checksum",
            saved_path=tmp_path / "staging" / "result.csv",
            error="upstream unavailable",
        ),
    )
    after_failure = repository.get_date_state(MARKET_DATE)

    assert after_failure is not None
    assert after_failure.status is PersistentSyncStatus.FILE_MISSING
    assert after_failure.evidence_state is SyncEvidenceState.LOCAL_FILE_MISSING
    assert after_failure.parsed_row_count == verified.parsed_row_count
    assert after_failure.valid_row_count == verified.valid_row_count
    assert after_failure.rejected_row_count == verified.rejected_row_count
    assert after_failure.csv_checksum_sha256 == saved.checksum
    assert after_failure.csv_relative_path == verified.csv_relative_path

    saved.path.write_bytes(original_bytes)
    restored = repository.prepare_fetch(MARKET_DAY, output_dir)
    restored_state = repository.get_date_state(MARKET_DATE)
    assert restored is not None and restored.status is DownloadStatus.ALREADY_PRESENT
    assert restored_state is not None
    assert restored_state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA


@pytest.mark.parametrize("candidate_matches", [False, True])
def test_in_flight_success_cannot_replace_newly_trusted_identity(
    tmp_path: Path,
    fixture_bytes,
    candidate_matches: bool,
) -> None:
    repository = make_repository(tmp_path)
    output_dir = tmp_path / "raw"
    assert repository.prepare_fetch(MARKET_DAY, output_dir) is None
    run_id = begin_sync_run(repository)

    trusted = make_valid_csv(output_dir, MARKET_DAY, fixture_bytes)
    repository.bootstrap_local_files(output_dir)
    trusted_state = repository.get_date_state(MARKET_DATE)
    assert trusted_state is not None
    candidate = (
        trusted
        if candidate_matches
        else make_valid_csv(
            tmp_path / "in-flight-candidate",
            MARKET_DAY,
            fixture_bytes,
            row_limit=1,
        )
    )
    candidate_rows = trusted_state.valid_row_count if candidate_matches else 1
    event = make_attempt(
        finished_at="2026-08-10T03:00:00+00:00",
        status=DownloadStatus.TRADING_DATA,
        http_status=200,
        classification="EQUITY_ROWS",
        valid_rows=candidate_rows,
        checksum=candidate.checksum,
        saved_path=candidate.path,
    )

    repository.record_attempt(run_id, event)
    after_attempt = repository.get_date_state(MARKET_DATE)
    repository.record_download_result(
        run_id,
        DownloadResult(
            requested_date=MARKET_DATE,
            status=DownloadStatus.TRADING_DATA,
            http_status=200,
            attempts=1,
            parsed_row_count=candidate_rows,
            valid_row_count=candidate_rows,
            saved_path=candidate.path,
            checksum=candidate.checksum,
        ),
    )
    final = repository.get_date_state(MARKET_DATE)

    expected_status = (
        PersistentSyncStatus.VERIFIED_TRADING_DATA
        if candidate_matches
        else PersistentSyncStatus.FILE_CONFLICT
    )
    assert after_attempt is not None and final is not None
    assert after_attempt.status is expected_status
    assert final.status is expected_status
    for state in (after_attempt, final):
        assert state.csv_checksum_sha256 == trusted.checksum
        assert state.csv_relative_path == trusted_state.csv_relative_path
        assert state.parsed_row_count == trusted_state.parsed_row_count
        assert state.valid_row_count == trusted_state.valid_row_count
        assert state.rejected_row_count == trusted_state.rejected_row_count
    if candidate_matches:
        assert final.last_error_type is None
        assert final.successful_attempt_count == 1
    else:
        assert final.evidence_state is SyncEvidenceState.LOCAL_CHECKSUM_CONFLICT
        assert final.last_error_type == "ARTIFACT_IDENTITY_CONFLICT"
        assert final.successful_attempt_count == 0


def test_new_attempt_clears_obsolete_reconciliation_cooldown(tmp_path: Path) -> None:
    repository = make_repository(tmp_path)
    first_run = begin_sync_run(repository)
    repository.record_attempt(
        first_run,
        make_attempt(
            finished_at="2026-08-08T01:00:00+00:00",
            status=DownloadStatus.HTTP_FAILURE,
            http_status=503,
            error_type="HTTP_FAILURE",
        ),
    )
    repository.set_recheck_after(
        MARKET_DATE,
        "2099-01-01T00:00:00+00:00",
        RECONCILIATION_POLICY_VERSION,
    )
    second_run = begin_sync_run(repository)
    repository.record_attempt(
        second_run,
        make_attempt(
            finished_at="2026-08-09T02:00:00+00:00",
            status=DownloadStatus.HTTP_FAILURE,
            http_status=503,
            error_type="HTTP_FAILURE",
        ),
    )

    state = repository.get_date_state(MARKET_DATE)
    assert state is not None
    assert state.next_recheck_after is None
    assert state.recheck_policy_version is None
