from __future__ import annotations

import asyncio
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import psx_data_sync.reconciliation as reconciliation_module
import psx_data_sync.state_db as state_db_module
from psx_data_sync.client import PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.reconciliation import (
    POLICY_VERSION,
    ReconciliationPlanner,
    reconcile_range,
)
from psx_data_sync.state import (
    ChecksumState,
    ClientFailureKind,
    DownloadAttemptEvent,
    DownloadResult,
    DownloadStatus,
    FetchResponse,
    FileHealthState,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationMode,
    WEEKEND_EMPTY_CLASSIFICATION_BASIS,
)
from psx_data_sync.state_db import StateDatabaseError, StateRepository
from psx_data_sync.validator import validate_rows


UTC = timezone.utc
NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)


def reconciliation_settings(
    tmp_path: Path,
    *,
    cooldown_seconds: float = 86_400,
    workers: int = 4,
    max_rechecks: int = 1,
) -> Settings:
    return Settings(
        raw_output_dir=tmp_path / "raw",
        state_db_path=tmp_path / "state" / "psx_sync.db",
        repair_staging_dir=tmp_path / "state" / "repair_staging",
        retry_attempts=3,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
        retry_jitter_fraction=0,
        range_workers=workers,
        max_rechecks_per_date_per_run=max_rechecks,
        reconciliation_cooldown_seconds=cooldown_seconds,
    )


def make_repository(settings: Settings, tmp_path: Path) -> StateRepository:
    repository = StateRepository(settings.state_db_path, project_root=tmp_path)
    repository.initialize()
    return repository


def valid_rows(fixture_bytes):
    return validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows


def save_valid_file(
    settings: Settings,
    market_date: date,
    fixture_bytes,
    *,
    row_limit: int | None = None,
    output_dir: Path | None = None,
):
    rows = valid_rows(fixture_bytes)
    if row_limit is not None:
        rows = rows[:row_limit]
    return save_canonical_csv(
        rows,
        market_date,
        settings.raw_output_dir if output_dir is None else output_dir,
    )


def attempt_event(
    market_date: date,
    *,
    attempt_number: int,
    finished_at: datetime,
    status: DownloadStatus,
    classification: str | None,
    http_status: int | None = 200,
    valid_row_count: int = 0,
    checksum: str | None = None,
    saved_path: Path | None = None,
) -> DownloadAttemptEvent:
    started_at = finished_at - timedelta(seconds=1)
    return DownloadAttemptEvent(
        requested_date=market_date.isoformat(),
        attempt_number=attempt_number,
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        duration_ms=1000,
        http_status=http_status,
        response_bytes=512,
        response_classification=classification,
        final_status=status,
        retryable=status is not DownloadStatus.TRADING_DATA,
        error_type=(None if status is DownloadStatus.TRADING_DATA else status.value),
        error_message=(None if status is DownloadStatus.TRADING_DATA else "fixture"),
        parsed_row_count=valid_row_count,
        valid_row_count=valid_row_count,
        checksum=checksum,
        saved_path=saved_path,
        worker_identifier="reconciliation-test",
    )


def record_observation_run(
    repository: StateRepository,
    market_date: date,
    observations: tuple[DownloadAttemptEvent, ...],
) -> str:
    run_id = repository.begin_sync_run(
        "fixture-observation",
        market_date.isoformat(),
        market_date.isoformat(),
        1,
        1,
    )
    for observation in observations:
        repository.record_attempt(run_id, observation)
    return run_id


def record_empty_run(
    repository: StateRepository,
    market_date: date,
    finished_at: datetime,
    *,
    attempts_in_run: int = 1,
) -> str:
    events = tuple(
        attempt_event(
            market_date,
            attempt_number=number,
            finished_at=finished_at + timedelta(seconds=number - 1),
            status=DownloadStatus.EMPTY_MARKET_RESPONSE,
            classification="EMPTY_MARKET_RESPONSE",
        )
        for number in range(1, attempts_in_run + 1)
    )
    return record_observation_run(repository, market_date, events)


def plan(
    repository: StateRepository,
    settings: Settings,
    dates,
    *,
    force_recheck: bool = False,
):
    return ReconciliationPlanner(
        settings,
        repository,
        now=NOW,
    ).plan(
        dates,
        run_id="planner-test",
        mode=ReconciliationMode.DRY_RUN,
        force_recheck=force_recheck,
    )


class FakeAsyncClient:
    def __init__(
        self,
        responses: dict[str, FetchResponse],
        *,
        delay_seconds: float = 0,
    ) -> None:
        self.responses = responses
        self.delay_seconds = delay_seconds
        self.calls: list[str] = []
        self.calls_by_date: defaultdict[str, int] = defaultdict(int)
        self.active = 0
        self.max_active = 0

    async def fetch(self, requested_date: date) -> FetchResponse:
        date_text = requested_date.isoformat()
        self.calls.append(date_text)
        self.calls_by_date[date_text] += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            return self.responses[date_text]
        finally:
            self.active -= 1


class NoNetworkClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, requested_date: date) -> FetchResponse:
        self.calls.append(requested_date.isoformat())
        raise AssertionError("dry reconciliation reached HTTP")


class CompleteThenBlockClient:
    def __init__(self, completed_date: date, response: FetchResponse) -> None:
        self.completed_date = completed_date
        self.response = response
        self.blocked = asyncio.Event()
        self.calls: list[str] = []

    async def fetch(self, requested_date: date) -> FetchResponse:
        self.calls.append(requested_date.isoformat())
        if requested_date == self.completed_date:
            return self.response
        self.blocked.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class SequenceAsyncClient:
    def __init__(self, outcomes: list[FetchResponse | Exception]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    async def fetch(self, requested_date: date) -> FetchResponse:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def one_row_response() -> FetchResponse:
    return FetchResponse(
        200,
        b"""
        <table><tbody>
          <tr data-type="equity">
            <td>ONLY</td><td>20</td><td>20</td><td>21</td><td>19</td>
            <td>20</td><td>0</td><td>0</td><td>100</td>
          </tr>
        </tbody></table>
        """,
    )


def test_never_attempted_weekday_and_weekend_are_virtual_and_sorted(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    friday = date(2026, 8, 7)
    saturday = date(2026, 8, 8)

    result = plan(repository, settings, (saturday, friday, saturday))

    assert result.requested_dates == ("2026-08-07", "2026-08-08")
    assert [item.market_date for item in result.results] == list(
        result.requested_dates
    )
    assert all(
        item.previous_status is PersistentSyncStatus.NEVER_ATTEMPTED
        and item.reconciled_status is PersistentSyncStatus.NEVER_ATTEMPTED
        and item.action_required is ReconciliationAction.NETWORK_RECHECK
        and item.network_recheck_required
        and item.recheck_eligible_now
        for item in result.results
    )
    weekday, weekend = result.results
    assert weekday.evidence_classification == "NO_OBSERVATION"
    assert not weekday.evidence_summary.calendar_weekend
    assert weekend.evidence_classification == "CALENDAR_SUPPORT_ONLY"
    assert weekend.evidence_summary.calendar_weekend
    assert weekend.evidence_summary.calendar_support == "SATURDAY_OR_SUNDAY"
    assert not result.complete
    assert result.resolution_percentage == 0
    assert repository.get_date_states_for_range(
        friday.isoformat(), saturday.isoformat()
    ) == {}


def test_large_dry_plan_uses_bounded_bulk_state_queries(
    tmp_path: Path, monkeypatch
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    dates = tuple(date(2016, 1, 1) + timedelta(days=offset) for offset in range(3_000))
    calls = {"states": 0, "evidence": 0}
    original_states = repository.get_date_states_for_range
    original_evidence = repository.get_attempt_evidence_for_range

    def counted_states(start_date: str, end_date: str):
        calls["states"] += 1
        return original_states(start_date, end_date)

    def counted_evidence(start_date: str, end_date: str):
        calls["evidence"] += 1
        return original_evidence(start_date, end_date)

    monkeypatch.setattr(repository, "get_date_states_for_range", counted_states)
    monkeypatch.setattr(
        repository, "get_attempt_evidence_for_range", counted_evidence
    )

    result = plan(repository, settings, dates)

    assert len(result.results) == 3_000
    assert calls == {"states": 1, "evidence": 1}
    assert repository.get_date_states_for_range(
        dates[0].isoformat(), dates[-1].isoformat()
    ) == {}


def test_weekend_empty_evidence_requires_distinct_runs_and_24_hours(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    saturday = date(2026, 8, 8)

    # Three HTTP retries in one command remain one independent observation.
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 8, 1, tzinfo=UTC),
        attempts_in_run=3,
    )
    first = plan(repository, settings, (saturday,)).results[0]
    assert first.empty_observation_count == 3
    assert first.evidence_summary.independent_empty_run_count == 1
    assert first.reconciled_status is PersistentSyncStatus.EMPTY_UNRESOLVED
    assert first.action_required is ReconciliationAction.NETWORK_RECHECK

    # A distinct command less than a day later is still not independent enough.
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 9, 0, tzinfo=UTC),
    )
    too_soon = plan(repository, settings, (saturday,)).results[0]
    assert too_soon.empty_observation_count == 4
    assert too_soon.evidence_summary.independent_empty_run_count == 1
    assert too_soon.reconciled_status is PersistentSyncStatus.EMPTY_UNRESOLVED

    # A later distinct run supplies the second policy-qualifying observation.
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 9, 2, tzinfo=UTC),
    )
    qualified = plan(repository, settings, (saturday,)).results[0]
    assert qualified.empty_observation_count == 5
    assert qualified.evidence_summary.independent_empty_run_count == 2
    assert qualified.reconciled_status is (
        PersistentSyncStatus.CONFIRMED_NON_TRADING
    )
    assert qualified.action_required is ReconciliationAction.CONFIRM_NON_TRADING
    assert not qualified.network_recheck_required


def test_repeated_weekday_empty_observations_remain_unresolved(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    friday = date(2026, 8, 7)
    for offset in range(3):
        record_empty_run(
            repository,
            friday,
            datetime(2026, 8, 7 + offset, 1, tzinfo=UTC),
        )

    item = plan(repository, settings, (friday,)).results[0]

    assert item.empty_observation_count == 3
    assert item.evidence_summary.independent_empty_run_count == 3
    assert item.reconciled_status is PersistentSyncStatus.EMPTY_UNRESOLVED
    assert item.action_required is ReconciliationAction.MANUAL_REVIEW
    assert not item.network_recheck_required
    assert any("holiday calendar" in warning for warning in item.warnings)

    forced = plan(
        repository, settings, (friday,), force_recheck=True
    ).results[0]
    assert forced.action_required is ReconciliationAction.MANUAL_REVIEW
    assert not forced.network_recheck_required


@pytest.mark.parametrize(
    ("failure_status", "http_status"),
    [
        (DownloadStatus.HTTP_FAILURE, 503),
        (DownloadStatus.PARSE_FAILURE, 200),
        (DownloadStatus.VALIDATION_FAILURE, 200),
    ],
)
def test_latest_failure_is_not_masked_by_older_empty_evidence(
    tmp_path: Path,
    failure_status: DownloadStatus,
    http_status: int,
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    record_empty_run(
        repository,
        market_day,
        datetime(2026, 8, 10, 1, tzinfo=UTC),
    )
    record_observation_run(
        repository,
        market_day,
        (
            attempt_event(
                market_day,
                attempt_number=1,
                finished_at=datetime(2026, 8, 11, 2, tzinfo=UTC),
                status=failure_status,
                classification=(
                    None
                    if failure_status is DownloadStatus.HTTP_FAILURE
                    else "UNEXPECTED_CONTENT"
                ),
                http_status=http_status,
            ),
        ),
    )

    item = plan(repository, settings, (market_day,)).results[0]

    assert item.reconciled_status is PersistentSyncStatus(failure_status.value)
    assert item.action_required is ReconciliationAction.NETWORK_RECHECK
    assert item.evidence_classification == "OBSERVED_RETRYABLE_FAILURE"
    assert item.empty_observation_count == 1


def test_valid_trading_observation_overrides_confirmed_non_trading(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    saturday = date(2026, 8, 8)
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 8, 1, tzinfo=UTC),
    )
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 9, 2, tzinfo=UTC),
    )
    before = repository.get_date_state(saturday)
    assert before is not None
    repository.confirm_non_trading(
        saturday.isoformat(),
        policy_version=POLICY_VERSION,
        classification_basis=WEEKEND_EMPTY_CLASSIFICATION_BASIS,
        expected_record_updated_at=before.record_updated_at,
        canonical_path=(
            settings.raw_output_dir / f"market_{saturday.isoformat()}.csv"
        ),
    )

    saved = save_valid_file(settings, saturday, fixture_bytes)
    record_observation_run(
        repository,
        saturday,
        (
            attempt_event(
                saturday,
                attempt_number=1,
                finished_at=datetime(2026, 8, 10, 3, tzinfo=UTC),
                status=DownloadStatus.TRADING_DATA,
                classification="EQUITY_ROWS",
                valid_row_count=3,
                checksum=saved.checksum,
                saved_path=saved.path,
            ),
        ),
    )

    item = plan(repository, settings, (saturday,)).results[0]
    state = repository.get_date_state(saturday)
    evidence = repository.get_attempt_evidence_for_range(
        saturday.isoformat(), saturday.isoformat()
    )[saturday.isoformat()]

    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert item.reconciled_status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert item.action_required is ReconciliationAction.NO_ACTION
    assert item.file_state is FileHealthState.HEALTHY
    assert item.checksum_state is ChecksumState.MATCH
    assert evidence.empty_observation_count == 2
    assert evidence.valid_observation_count == 1
    assert item.valid_observation_count == 1


def test_planner_artifact_matrix_is_read_only(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    healthy_day = date(2026, 8, 3)
    missing_day = date(2026, 8, 4)
    corrupt_day = date(2026, 8, 5)
    conflict_day = date(2026, 8, 6)
    unindexed_day = date(2026, 8, 7)

    healthy = save_valid_file(settings, healthy_day, fixture_bytes)
    missing = save_valid_file(settings, missing_day, fixture_bytes)
    corrupt = save_valid_file(settings, corrupt_day, fixture_bytes)
    conflict = save_valid_file(settings, conflict_day, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    healthy_bytes = healthy.path.read_bytes()

    missing.path.unlink()
    corrupt_bytes = b"not,a,canonical,csv\n"
    corrupt.path.write_bytes(corrupt_bytes)
    alternate_dir = tmp_path / "alternate"
    alternate = save_valid_file(
        settings,
        conflict_day,
        fixture_bytes,
        row_limit=1,
        output_dir=alternate_dir,
    )
    conflicting_bytes = alternate.path.read_bytes()
    conflict.path.write_bytes(conflicting_bytes)
    unindexed = save_valid_file(settings, unindexed_day, fixture_bytes)

    before = repository.get_date_states_for_range(
        healthy_day.isoformat(), unindexed_day.isoformat()
    )
    result = plan(
        repository,
        settings,
        (unindexed_day, conflict_day, corrupt_day, missing_day, healthy_day),
    )
    items = {item.market_date: item for item in result.results}

    healthy_item = items[healthy_day.isoformat()]
    assert healthy_item.reconciled_status is (
        PersistentSyncStatus.VERIFIED_TRADING_DATA
    )
    assert healthy_item.action_required is ReconciliationAction.NO_ACTION
    assert healthy_item.file_state is FileHealthState.HEALTHY
    assert healthy_item.checksum_state is ChecksumState.MATCH

    missing_item = items[missing_day.isoformat()]
    assert missing_item.reconciled_status is PersistentSyncStatus.FILE_MISSING
    assert missing_item.action_required is ReconciliationAction.REPAIR_MISSING_FILE
    assert missing_item.file_state is FileHealthState.MISSING
    assert missing_item.evidence_summary.expected_checksum == missing.checksum

    corrupt_item = items[corrupt_day.isoformat()]
    assert corrupt_item.reconciled_status is PersistentSyncStatus.FILE_CORRUPT
    assert corrupt_item.action_required is (
        ReconciliationAction.INVESTIGATE_CORRUPT_FILE
    )
    assert corrupt_item.file_state is FileHealthState.CORRUPT
    assert corrupt_item.evidence_summary.expected_checksum == corrupt.checksum

    conflict_item = items[conflict_day.isoformat()]
    assert conflict_item.reconciled_status is PersistentSyncStatus.FILE_CONFLICT
    assert conflict_item.action_required is (
        ReconciliationAction.INVESTIGATE_CONFLICT
    )
    assert conflict_item.file_state is FileHealthState.CONFLICT
    assert conflict_item.checksum_state is ChecksumState.MISMATCH
    assert conflict_item.evidence_summary.expected_checksum == conflict.checksum

    unindexed_item = items[unindexed_day.isoformat()]
    assert unindexed_item.reconciled_status is (
        PersistentSyncStatus.VERIFIED_TRADING_DATA
    )
    assert unindexed_item.action_required is ReconciliationAction.LOCAL_REINDEX
    assert unindexed_item.file_state is FileHealthState.UNTRACKED_VALID
    assert unindexed_item.checksum_state is ChecksumState.UNTRACKED
    assert unindexed_item.evidence_summary.observed_checksum == unindexed.checksum
    assert not unindexed_item.resolved

    # Pure planning does not rewrite state or any local evidence.
    after = repository.get_date_states_for_range(
        healthy_day.isoformat(), unindexed_day.isoformat()
    )
    assert after == before
    assert repository.get_date_state(unindexed_day) is None
    assert healthy.path.read_bytes() == healthy_bytes
    assert not missing.path.exists()
    assert corrupt.path.read_bytes() == corrupt_bytes
    assert conflict.path.read_bytes() == conflicting_bytes
    assert not result.complete
    assert result.file_health_issue_count == 3


def test_unanchored_persisted_conflict_is_never_adopted_automatically(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    saved = save_valid_file(settings, market_day, fixture_bytes)
    repository.mark_artifact_issue(
        market_day.isoformat(),
        PersistentSyncStatus.FILE_CONFLICT,
        error_type="UNANCHORED_CONFLICT",
        error_message="two untrusted candidates disagree",
        expected_artifact_path=saved.path,
        expected_artifact_exists=True,
        expected_artifact_valid=True,
        expected_observed_checksum=saved.checksum,
    )

    item = plan(repository, settings, (market_day,)).results[0]
    preflight = repository.prepare_fetch(market_day, settings.raw_output_dir)
    state = repository.get_date_state(market_day)

    assert item.reconciled_status is PersistentSyncStatus.FILE_CONFLICT
    assert item.action_required is ReconciliationAction.INVESTIGATE_CONFLICT
    assert item.evidence_classification == "PERSISTED_UNANCHORED_CONFLICT"
    assert preflight is not None
    assert preflight.status is DownloadStatus.FILE_CONFLICT
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_CONFLICT
    assert state.last_verified_at is None
    assert state.csv_checksum_sha256 is None


@pytest.mark.asyncio
async def test_dry_run_performs_zero_http_and_no_destructive_state_writes(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    client = NoNetworkClient()

    result = await reconcile_range(
        settings,
        repository,
        "2026-08-07",
        "2026-08-08",
        client=client,
        now=NOW,
    )

    assert result.mode is ReconciliationMode.DRY_RUN
    assert client.calls == []
    assert repository.get_date_states_for_range("2026-08-07", "2026-08-08") == {}
    assert not settings.raw_output_dir.exists()
    assert not settings.repair_staging_dir.exists()
    with sqlite3.connect(settings.state_db_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM download_attempts"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM sync_runs"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM reconciliation_events"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM reconciliation_runs"
        ).fetchone()[0] == 1


def test_cooldown_and_force_recheck_scope(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    unresolved_day = date(2026, 8, 10)
    failure_day = date(2026, 8, 11)
    missing_day = date(2026, 8, 12)
    healthy_day = date(2026, 8, 13)
    never_day = date(2026, 8, 14)
    recent = datetime(2026, 8, 19, 6, tzinfo=UTC)

    record_empty_run(repository, unresolved_day, recent)
    record_observation_run(
        repository,
        failure_day,
        (
            attempt_event(
                failure_day,
                attempt_number=1,
                finished_at=recent,
                status=DownloadStatus.HTTP_FAILURE,
                classification=None,
                http_status=503,
            ),
        ),
    )
    missing = save_valid_file(settings, missing_day, fixture_bytes)
    save_valid_file(settings, healthy_day, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    missing_state_run = repository.begin_sync_run(
        "staged-fixture",
        missing_day.isoformat(),
        missing_day.isoformat(),
        1,
        1,
    )
    repository.record_staged_attempt(
        missing_state_run,
        attempt_event(
            missing_day,
            attempt_number=1,
            finished_at=recent,
            status=DownloadStatus.TEMPORARY_FAILURE,
            classification=None,
            http_status=None,
        ),
    )
    missing.path.unlink()
    dates = (unresolved_day, failure_day, missing_day, healthy_day, never_day)

    ordinary = {
        item.market_date: item
        for item in plan(repository, settings, dates).results
    }
    forced = {
        item.market_date: item
        for item in plan(
            repository,
            settings,
            dates,
            force_recheck=True,
        ).results
    }

    for day in (unresolved_day, failure_day):
        normal = ordinary[day.isoformat()]
        override = forced[day.isoformat()]
        assert normal.network_recheck_required
        assert not normal.recheck_eligible_now
        assert normal.next_recheck_after is not None
        assert override.network_recheck_required
        assert override.recheck_eligible_now
        assert override.next_recheck_after is None

    normal_missing = ordinary[missing_day.isoformat()]
    forced_missing = forced[missing_day.isoformat()]
    assert normal_missing.action_required is ReconciliationAction.REPAIR_MISSING_FILE
    assert normal_missing.recheck_eligible_now
    assert forced_missing.recheck_eligible_now
    assert forced_missing.next_recheck_after is None

    assert ordinary[healthy_day.isoformat()].action_required is (
        ReconciliationAction.NO_ACTION
    )
    assert forced[healthy_day.isoformat()].action_required is (
        ReconciliationAction.NO_ACTION
    )
    assert not forced[healthy_day.isoformat()].network_recheck_required
    assert ordinary[never_day.isoformat()].recheck_eligible_now
    assert forced[never_day.isoformat()].recheck_eligible_now


def test_latest_attempt_extends_an_older_persisted_cooldown(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    record_empty_run(
        repository,
        market_day,
        datetime(2026, 8, 18, 1, tzinfo=UTC),
    )
    state = repository.get_date_state(market_day)
    assert state is not None
    repository.set_recheck_after(
        market_day.isoformat(),
        datetime(2026, 8, 19, 1, tzinfo=UTC).isoformat(),
        POLICY_VERSION,
        expected_record_updated_at=state.record_updated_at,
        expected_status=state.status,
    )
    record_observation_run(
        repository,
        market_day,
        (
            attempt_event(
                market_day,
                attempt_number=1,
                finished_at=datetime(2026, 8, 19, 11, tzinfo=UTC),
                status=DownloadStatus.HTTP_FAILURE,
                classification=None,
                http_status=503,
            ),
        ),
    )

    item = plan(repository, settings, (market_day,)).results[0]

    assert item.reconciled_status is PersistentSyncStatus.HTTP_FAILURE
    assert not item.recheck_eligible_now
    assert item.next_recheck_after == datetime(
        2026, 8, 20, 11, tzinfo=UTC
    ).isoformat(timespec="microseconds")


def test_materialized_never_attempted_state_remains_network_eligible(
    tmp_path: Path,
) -> None:
    settings = reconciliation_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    run_id = repository.begin_sync_run(
        "fixture", market_day.isoformat(), market_day.isoformat(), 1, 1
    )
    repository.record_download_result(
        run_id,
        DownloadResult(
            requested_date=market_day.isoformat(),
            status=DownloadStatus.INVALID_DATE,
            error="synthetic materialized gap",
        ),
    )

    state = repository.get_date_state(market_day)
    item = plan(repository, settings, (market_day,)).results[0]

    assert state is not None
    assert state.status is PersistentSyncStatus.NEVER_ATTEMPTED
    assert state.attempt_count == 0
    assert item.reconciled_status is PersistentSyncStatus.NEVER_ATTEMPTED
    assert item.action_required is ReconciliationAction.NETWORK_RECHECK
    assert item.recheck_eligible_now


@pytest.mark.asyncio
async def test_apply_targets_only_eligible_dates_and_keeps_network_concurrent(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, workers=2)
    repository = make_repository(settings, tmp_path)
    healthy_day = date(2026, 8, 3)
    cooling_day = date(2026, 8, 4)
    first_gap = date(2026, 8, 5)
    second_gap = date(2026, 8, 6)
    healthy = save_valid_file(settings, healthy_day, fixture_bytes)
    healthy_before = healthy.path.read_bytes()
    repository.bootstrap_local_files(settings.raw_output_dir)
    record_empty_run(
        repository,
        cooling_day,
        datetime(2026, 8, 19, 6, tzinfo=UTC),
    )
    response = FetchResponse(200, fixture_bytes("valid_market.html"))
    client = FakeAsyncClient(
        {
            first_gap.isoformat(): response,
            second_gap.isoformat(): response,
        },
        delay_seconds=0.02,
    )

    result = await reconcile_range(
        settings,
        repository,
        healthy_day.isoformat(),
        second_gap.isoformat(),
        apply=True,
        workers=2,
        client=client,
        now=NOW,
    )

    assert set(client.calls) == {first_gap.isoformat(), second_gap.isoformat()}
    assert client.calls_by_date[first_gap.isoformat()] == 1
    assert client.calls_by_date[second_gap.isoformat()] == 1
    assert client.calls_by_date[healthy_day.isoformat()] == 0
    assert client.calls_by_date[cooling_day.isoformat()] == 0
    assert client.max_active == 2
    assert result.network_recheck_planned_count == 3
    assert result.network_recheck_count == 2
    assert result.network_rechecked_dates == (
        first_gap.isoformat(),
        second_gap.isoformat(),
    )
    assert result.staged_repair_dates == result.network_rechecked_dates
    assert result.promoted_repair_dates == result.network_rechecked_dates
    assert healthy.path.read_bytes() == healthy_before
    for day in (first_gap, second_gap):
        canonical = settings.raw_output_dir / f"market_{day.isoformat()}.csv"
        staged = (
            settings.repair_staging_dir
            / result.run_id
            / f"market_{day.isoformat()}.csv"
        )
        assert canonical.exists()
        assert staged.exists()
        assert canonical.read_bytes() == staged.read_bytes()
        state = repository.get_date_state(day)
        assert state is not None
        assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert repository.get_date_state(cooling_day).status is (
        PersistentSyncStatus.EMPTY_UNRESOLVED
    )
    assert not result.complete


@pytest.mark.asyncio
async def test_valid_rows_with_save_failure_can_recover_as_first_artifact(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    record_observation_run(
        repository,
        market_day,
        (
            attempt_event(
                market_day,
                attempt_number=1,
                finished_at=datetime(2026, 8, 10, 1, tzinfo=UTC),
                status=DownloadStatus.SAVE_FAILURE,
                classification="EQUITY_ROWS",
                valid_row_count=3,
                checksum=None,
                saved_path=None,
            ),
        ),
    )
    before = plan(repository, settings, (market_day,)).results[0]
    assert before.reconciled_status is PersistentSyncStatus.TEMPORARY_FAILURE
    assert before.action_required is ReconciliationAction.NETWORK_RECHECK
    assert before.evidence_classification == "OBSERVED_RETRYABLE_FAILURE"
    assert before.valid_observation_count == 1

    client = FakeAsyncClient(
        {market_day.isoformat(): FetchResponse(200, fixture_bytes("valid_market.html"))}
    )
    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    state = repository.get_date_state(market_day)
    assert client.calls == [market_day.isoformat()]
    assert canonical.exists()
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.csv_checksum_sha256 is not None
    assert result.complete
    assert result.status_transition_count == 1
    assert result.promoted_repair_dates == (market_day.isoformat(),)


@pytest.mark.asyncio
async def test_interrupted_apply_keeps_completed_work_and_cancelled_date_resumable(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=86_400, workers=1)
    repository = make_repository(settings, tmp_path)
    completed_day = date(2026, 8, 10)
    cancelled_day = date(2026, 8, 11)
    client = CompleteThenBlockClient(
        completed_day,
        FetchResponse(200, fixture_bytes("valid_market.html")),
    )

    task = asyncio.create_task(
        reconcile_range(
            settings,
            repository,
            completed_day.isoformat(),
            cancelled_day.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )
    )
    await asyncio.wait_for(client.blocked.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    completed_state = repository.get_date_state(completed_day)
    cancelled_state = repository.get_date_state(cancelled_day)
    assert completed_state is not None
    assert completed_state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert cancelled_state is not None
    assert cancelled_state.last_error_type == "CANCELLED"
    assert not (
        settings.raw_output_dir / f"market_{cancelled_day.isoformat()}.csv"
    ).exists()
    resume = plan(repository, settings, (completed_day, cancelled_day))
    completed_item, cancelled_item = resume.results
    assert completed_item.action_required is ReconciliationAction.NO_ACTION
    assert cancelled_item.action_required is ReconciliationAction.NETWORK_RECHECK
    assert cancelled_item.recheck_eligible_now
    assert cancelled_item.next_recheck_after is None

    with sqlite3.connect(settings.state_db_path) as connection:
        connection.row_factory = sqlite3.Row
        reconciliation_run = connection.execute(
            "SELECT * FROM reconciliation_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        child_run = connection.execute(
            "SELECT * FROM sync_runs WHERE command_type = 'reconcile-recheck'"
        ).fetchone()
        attempts = connection.execute(
            "SELECT market_date, error_type FROM download_attempts ORDER BY id"
        ).fetchall()
        claims = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_recheck_claims"
        ).fetchone()[0]
    assert reconciliation_run is not None
    assert reconciliation_run["status"] == "INTERRUPTED"
    assert reconciliation_run["interrupted"] == 1
    assert reconciliation_run["network_recheck_count"] == 2
    assert reconciliation_run["status_transition_count"] >= 1
    assert child_run is not None
    assert child_run["status"] == "INTERRUPTED"
    assert child_run["network_fetch_count"] == 2
    assert child_run["total_attempts"] == 2
    assert [(row["market_date"], row["error_type"]) for row in attempts] == [
        (completed_day.isoformat(), None),
        (cancelled_day.isoformat(), "CANCELLED"),
    ]
    assert claims == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("http_status", [429, 503])
async def test_repeated_retryable_http_failure_is_audited_and_cooled_down(
    tmp_path: Path,
    http_status: int,
) -> None:
    settings = reconciliation_settings(tmp_path, max_rechecks=2)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    failures = [
        PSXClientError(
            f"HTTP {http_status}",
            kind=ClientFailureKind.HTTP,
            retryable=True,
            http_status=http_status,
        )
        for _ in range(2)
    ]
    client = SequenceAsyncClient(failures)

    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    state = repository.get_date_state(market_day)
    evidence = repository.get_attempt_evidence_for_range(
        market_day.isoformat(), market_day.isoformat()
    )[market_day.isoformat()]
    item = result.results[0]
    assert client.calls == 2
    assert state is not None
    assert state.status is PersistentSyncStatus.HTTP_FAILURE
    assert evidence.http_statuses == (http_status, http_status)
    assert evidence.attempt_count == 2
    assert item.reconciled_status is PersistentSyncStatus.HTTP_FAILURE
    assert item.action_required is ReconciliationAction.NETWORK_RECHECK
    assert not item.recheck_eligible_now
    assert item.next_recheck_after is not None
    assert result.network_recheck_count == 1
    assert not result.complete
    assert not (
        settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    ).exists()
    with sqlite3.connect(settings.state_db_path) as connection:
        child = connection.execute(
            "SELECT total_attempts, network_fetch_count FROM sync_runs "
            "WHERE command_type = 'reconcile-recheck'"
        ).fetchone()
        candidate = connection.execute(
            "SELECT disposition FROM repair_candidates "
            "WHERE reconciliation_run_id = ?",
            (result.run_id,),
        ).fetchone()
    assert child == (2, 1)
    assert candidate == ("NO_VALID_STAGED_ARTIFACT",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_name", "expected_status", "expected_attempts"),
    [
        ("malformed.html", PersistentSyncStatus.PARSE_FAILURE, 2),
        ("malformed_numeric.html", PersistentSyncStatus.VALIDATION_FAILURE, 1),
    ],
)
async def test_parser_and_validation_failures_remain_retryable_without_promotion(
    tmp_path: Path,
    fixture_bytes,
    fixture_name: str,
    expected_status: PersistentSyncStatus,
    expected_attempts: int,
) -> None:
    settings = reconciliation_settings(tmp_path, max_rechecks=2)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    client = SequenceAsyncClient(
        [
            FetchResponse(200, fixture_bytes(fixture_name))
            for _ in range(expected_attempts)
        ]
    )

    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    state = repository.get_date_state(market_day)
    evidence = repository.get_attempt_evidence_for_range(
        market_day.isoformat(), market_day.isoformat()
    )[market_day.isoformat()]
    item = result.results[0]
    assert client.calls == expected_attempts
    assert state is not None
    assert state.status is expected_status
    assert evidence.attempt_count == expected_attempts
    assert evidence.http_statuses == (200,) * expected_attempts
    assert item.reconciled_status is expected_status
    assert item.action_required is ReconciliationAction.NETWORK_RECHECK
    assert not item.recheck_eligible_now
    assert item.next_recheck_after is not None
    assert not result.complete
    assert result.promoted_repair_dates == ()
    assert not (
        settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    ).exists()


@pytest.mark.asyncio
async def test_missing_file_exact_historical_identity_is_staged_then_promoted(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 11)
    original = save_valid_file(settings, market_day, fixture_bytes)
    original_bytes = original.path.read_bytes()
    repository.bootstrap_local_files(settings.raw_output_dir)
    original.path.unlink()
    client = FakeAsyncClient(
        {market_day.isoformat(): FetchResponse(200, fixture_bytes("valid_market.html"))}
    )

    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    staged_path = (
        settings.repair_staging_dir
        / result.run_id
        / f"market_{market_day.isoformat()}.csv"
    )
    state = repository.get_date_state(market_day)
    assert client.calls == [market_day.isoformat()]
    assert result.staged_repair_dates == (market_day.isoformat(),)
    assert result.promoted_repair_dates == (market_day.isoformat(),)
    assert original.path.read_bytes() == original_bytes
    assert staged_path.read_bytes() == original_bytes
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert state.csv_checksum_sha256 == original.checksum
    assert result.complete
    assert result.status_transition_count == 2
    events = repository.list_reconciliation_events(result.run_id)
    assert [
        (event["previous_status"], event["new_status"], event["action"])
        for event in events
    ] == [
        (
            "VERIFIED_TRADING_DATA",
            "FILE_MISSING",
            "REPAIR_MISSING_FILE",
        ),
        (
            "FILE_MISSING",
            "VERIFIED_TRADING_DATA",
            "REPAIR_MISSING_FILE",
        ),
    ]
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT prior_checksum_sha256, candidate_checksum_sha256,
                   disposition, promoted_at
            FROM repair_candidates WHERE reconciliation_run_id = ?
            """,
            (result.run_id,),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == candidate[1] == original.checksum
    assert candidate[2] == "PROMOTED"
    assert candidate[3] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first_outcome", "expected_previous_status"),
    [
        ("HTTP_503", PersistentSyncStatus.HTTP_FAILURE),
        ("EMPTY", PersistentSyncStatus.EMPTY_UNRESOLVED),
    ],
)
async def test_same_run_retry_promotes_from_current_transient_state(
    tmp_path: Path,
    fixture_bytes,
    first_outcome: str,
    expected_previous_status: PersistentSyncStatus,
) -> None:
    settings = reconciliation_settings(
        tmp_path,
        cooldown_seconds=0,
        max_rechecks=2,
    )
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    first = (
        PSXClientError(
            "HTTP 503",
            kind=ClientFailureKind.HTTP,
            retryable=True,
            http_status=503,
        )
        if first_outcome == "HTTP_503"
        else FetchResponse(200, fixture_bytes("empty_shell.html"))
    )
    client = SequenceAsyncClient(
        [first, FetchResponse(200, fixture_bytes("valid_market.html"))]
    )

    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    state = repository.get_date_state(market_day)
    assert client.calls == 2
    assert canonical.exists()
    assert state is not None
    assert state.status is PersistentSyncStatus.VERIFIED_TRADING_DATA
    assert result.complete
    assert result.promoted_repair_dates == (market_day.isoformat(),)
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT disposition, promoted_at FROM repair_candidates
            WHERE reconciliation_run_id = ? AND market_date = ?
            """,
            (result.run_id, market_day.isoformat()),
        ).fetchone()
        run_status = connection.execute(
            "SELECT status FROM reconciliation_runs WHERE run_id = ?",
            (result.run_id,),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == "PROMOTED"
    assert candidate[1] is not None
    assert run_status == ("COMPLETED",)
    events = repository.list_reconciliation_events(result.run_id)
    assert [
        (
            event["previous_status"],
            event["new_status"],
            event["action"],
            event["evidence_classification"],
        )
        for event in events
    ] == [
        (
            expected_previous_status.value,
            PersistentSyncStatus.VERIFIED_TRADING_DATA.value,
            ReconciliationAction.NETWORK_RECHECK.value,
            "STAGED_CANONICAL_PROMOTED",
        )
    ]


@pytest.mark.asyncio
async def test_missing_file_different_staged_identity_is_never_promoted(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 11)
    original = save_valid_file(settings, market_day, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    original.path.unlink()
    client = FakeAsyncClient({market_day.isoformat(): one_row_response()})

    result = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=client,
        now=NOW,
    )

    staged_path = (
        settings.repair_staging_dir
        / result.run_id
        / f"market_{market_day.isoformat()}.csv"
    )
    state = repository.get_date_state(market_day)
    item = result.results[0]
    assert not original.path.exists()
    assert staged_path.exists()
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_CONFLICT
    assert state.csv_checksum_sha256 == original.checksum
    assert state.last_error_type == "HISTORICAL_MISMATCH"
    assert item.reconciled_status is PersistentSyncStatus.FILE_CONFLICT
    assert item.action_required is ReconciliationAction.INVESTIGATE_CONFLICT
    assert item.file_state is FileHealthState.CONFLICT
    assert result.promoted_repair_dates == ()
    assert not result.complete
    assert result.status_transition_count == 2
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT prior_checksum_sha256, candidate_checksum_sha256,
                   disposition, promoted_at
            FROM repair_candidates WHERE reconciliation_run_id = ?
            """,
            (result.run_id,),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == original.checksum
    assert candidate[1] != original.checksum
    assert candidate[2] == "HISTORICAL_MISMATCH"
    assert candidate[3] is None
    events = repository.list_reconciliation_events(result.run_id)
    assert [
        (
            event["previous_status"],
            event["new_status"],
            event["action"],
            event["evidence_classification"],
        )
        for event in events
    ] == [
        (
            "VERIFIED_TRADING_DATA",
            "FILE_MISSING",
            "REPAIR_MISSING_FILE",
            "HISTORICAL_TRADING_ARTIFACT_MISSING",
        ),
        (
            "FILE_MISSING",
            "FILE_CONFLICT",
            "REPAIR_MISSING_FILE",
            "HISTORICAL_MISMATCH",
        ),
    ]


@pytest.mark.asyncio
async def test_promotion_db_failure_keeps_durable_intent_and_is_recoverable(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    response = FetchResponse(200, fixture_bytes("valid_market.html"))
    first_client = FakeAsyncClient({market_day.isoformat(): response})
    real_insert = state_db_module._insert_reconciliation_event

    def fail_promoted_event(connection, run_id, decision):
        if decision.evidence_classification == "STAGED_CANONICAL_PROMOTED":
            raise sqlite3.OperationalError("injected finalization failure")
        return real_insert(connection, run_id, decision)

    with monkeypatch.context() as scoped:
        scoped.setattr(
            state_db_module,
            "_insert_reconciliation_event",
            fail_promoted_event,
        )
        with pytest.raises(sqlite3.OperationalError, match="injected finalization"):
            await reconcile_range(
                settings,
                repository,
                market_day.isoformat(),
                market_day.isoformat(),
                apply=True,
                workers=1,
                client=first_client,
                now=NOW,
            )

    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    with sqlite3.connect(settings.state_db_path) as connection:
        pending = connection.execute(
            """
            SELECT reconciliation_run_id, disposition, promoted_at,
                   candidate_checksum_sha256
            FROM repair_candidates WHERE market_date = ?
            """,
            (market_day.isoformat(),),
        ).fetchone()
        candidate_count = connection.execute(
            "SELECT COUNT(*) FROM repair_candidates WHERE market_date = ?",
            (market_day.isoformat(),),
        ).fetchone()[0]
    assert candidate_count == 1
    assert pending is not None
    failed_run_id, disposition, promoted_at, candidate_checksum = pending
    assert disposition == "PENDING_PROMOTION"
    assert promoted_at is None
    assert repository.list_reconciliation_events(failed_run_id) == ()
    assert canonical.exists()
    assert candidate_checksum is not None
    staged = (
        settings.repair_staging_dir
        / failed_run_id
        / f"market_{market_day.isoformat()}.csv"
    )
    assert staged.exists()
    state_after_failure = repository.get_date_state(market_day)
    assert state_after_failure is not None
    assert state_after_failure.status is not (
        PersistentSyncStatus.VERIFIED_TRADING_DATA
    )

    resume_client = NoNetworkClient()
    resumed = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=resume_client,
        now=NOW,
    )

    assert resume_client.calls == []
    assert resumed.complete
    assert repository.get_date_state(market_day).status is (
        PersistentSyncStatus.VERIFIED_TRADING_DATA
    )
    assert canonical.read_bytes() == staged.read_bytes()
    with sqlite3.connect(settings.state_db_path) as connection:
        recovered = connection.execute(
            """
            SELECT disposition, promoted_at FROM repair_candidates
            WHERE reconciliation_run_id = ? AND market_date = ?
            """,
            (failed_run_id, market_day.isoformat()),
        ).fetchone()
    assert recovered == ("RECOVERED_CANONICAL_MATCH", None)
    assert any("RECOVERED_CANONICAL_MATCH" in warning for warning in resumed.warnings)


@pytest.mark.asyncio
async def test_pending_staged_candidate_resumes_without_another_download(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    response = FetchResponse(200, fixture_bytes("valid_market.html"))

    def fail_before_promotion(*args, **kwargs):
        raise OSError("injected failure before promotion")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            reconciliation_module,
            "promote_staged_csv_if_safe",
            fail_before_promotion,
        )
        with pytest.raises(OSError, match="before promotion"):
            await reconcile_range(
                settings,
                repository,
                market_day.isoformat(),
                market_day.isoformat(),
                apply=True,
                workers=1,
                client=FakeAsyncClient({market_day.isoformat(): response}),
                now=NOW,
            )

    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    assert not canonical.exists()
    resume_client = NoNetworkClient()
    resumed = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=resume_client,
        now=NOW,
    )

    assert resume_client.calls == []
    assert canonical.exists()
    assert resumed.complete
    assert resumed.promoted_repair_dates == (market_day.isoformat(),)
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT disposition, promoted_at FROM repair_candidates
            WHERE market_date = ? ORDER BY id
            """,
            (market_day.isoformat(),),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == "RECOVERY_PROMOTED"
    assert candidate[1] is not None
    assert any("RECOVERY_PROMOTED" in warning for warning in resumed.warnings)


def test_recovery_does_not_terminalize_a_live_owner_intent(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    record_observation_run(
        repository,
        market_day,
        (
            attempt_event(
                market_day,
                attempt_number=1,
                finished_at=NOW - timedelta(hours=1),
                status=DownloadStatus.HTTP_FAILURE,
                classification=None,
                http_status=503,
            ),
        ),
    )
    owner_run_id = repository.begin_reconciliation_run(
        policy_version=POLICY_VERSION,
        start_date=market_day.isoformat(),
        end_date=market_day.isoformat(),
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
        cooldown_seconds=0,
    )
    claim_time = datetime.now(UTC)
    assert repository.claim_network_rechecks(
        owner_run_id,
        (market_day.isoformat(),),
        claimed_at=claim_time.isoformat(),
        expires_at=(claim_time + timedelta(hours=1)).isoformat(),
    ) == (market_day.isoformat(),)
    staged = save_valid_file(
        settings,
        market_day,
        fixture_bytes,
        output_dir=settings.repair_staging_dir / owner_run_id,
    )
    repository.begin_repair_candidate(
        owner_run_id,
        market_day.isoformat(),
        staged.path,
        prior_checksum=None,
        candidate_checksum=staged.checksum,
        prior_row_count=None,
        candidate_row_count=len(valid_rows(fixture_bytes)),
    )
    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    canonical.parent.mkdir(parents=True, exist_ok=True)
    canonical.write_bytes(staged.path.read_bytes())
    recovery_run_id = repository.begin_reconciliation_run(
        policy_version=POLICY_VERSION,
        start_date=market_day.isoformat(),
        end_date=market_day.isoformat(),
        mode=ReconciliationMode.APPLY,
        requested_date_count=1,
        worker_count=1,
        force_recheck=False,
        max_rechecks_per_date=1,
        cooldown_seconds=0,
    )

    assert repository.recover_pending_repair_candidates(
        market_day.isoformat(),
        market_day.isoformat(),
        settings.raw_output_dir,
        reconciliation_run_id=recovery_run_id,
    ) == ()
    with sqlite3.connect(settings.state_db_path) as connection:
        assert connection.execute(
            "SELECT disposition FROM repair_candidates WHERE market_date = ?",
            (market_day.isoformat(),),
        ).fetchone() == ("PENDING_PROMOTION",)

    repository.mark_reconciliation_run_failed(
        owner_run_id,
        interrupted=False,
        duration_ms=1,
        error_message="injected owner failure",
    )
    assert repository.recover_pending_repair_candidates(
        market_day.isoformat(),
        market_day.isoformat(),
        settings.raw_output_dir,
        reconciliation_run_id=recovery_run_id,
    ) == ((market_day.isoformat(), "RECOVERED_CANONICAL_MATCH"),)


@pytest.mark.asyncio
async def test_destination_race_is_detected_after_write_ahead_intent(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 10)
    client = FakeAsyncClient(
        {market_day.isoformat(): FetchResponse(200, fixture_bytes("valid_market.html"))}
    )
    canonical = settings.raw_output_dir / f"market_{market_day.isoformat()}.csv"
    concurrent_file = save_valid_file(
        settings,
        market_day,
        fixture_bytes,
        row_limit=1,
        output_dir=tmp_path / "concurrent",
    )
    concurrent_bytes = concurrent_file.path.read_bytes()
    real_promote = reconciliation_module.promote_staged_csv_if_safe

    def race_during_promotion(*args, **kwargs):
        canonical.parent.mkdir(parents=True, exist_ok=True)
        canonical.write_bytes(concurrent_bytes)
        return real_promote(*args, **kwargs)

    monkeypatch.setattr(
        reconciliation_module,
        "promote_staged_csv_if_safe",
        race_during_promotion,
    )

    with pytest.raises(
        StateDatabaseError, match="destination appeared during promotion"
    ):
        await reconcile_range(
            settings,
            repository,
            market_day.isoformat(),
            market_day.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )

    assert canonical.read_bytes() == concurrent_bytes
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT disposition, promoted_at, staged_relative_path
            FROM repair_candidates WHERE market_date = ?
            """,
            (market_day.isoformat(),),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == "PENDING_PROMOTION"
    assert candidate[1] is None
    assert (tmp_path / candidate[2]).exists()

    resume_client = NoNetworkClient()
    resumed = await reconcile_range(
        settings,
        repository,
        market_day.isoformat(),
        market_day.isoformat(),
        apply=True,
        workers=1,
        client=resume_client,
        now=NOW,
    )

    assert resume_client.calls == []
    assert canonical.read_bytes() == concurrent_bytes
    assert not resumed.complete
    state = repository.get_date_state(market_day)
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_CONFLICT
    with sqlite3.connect(settings.state_db_path) as connection:
        recovered = connection.execute(
            """
            SELECT disposition, promoted_at FROM repair_candidates
            WHERE market_date = ?
            """,
            (market_day.isoformat(),),
        ).fetchone()
        recovery_event = connection.execute(
            """
            SELECT new_status, action, evidence_classification
            FROM reconciliation_events
            WHERE run_id = ? AND market_date = ?
            """,
            (resumed.run_id, market_day.isoformat()),
        ).fetchone()
    assert recovered == ("RECOVERY_DESTINATION_CONFLICT", None)
    assert recovery_event == (
        "FILE_CONFLICT",
        "INVESTIGATE_CONFLICT",
        "RECOVERY_DESTINATION_CONFLICT",
    )
    assert any(
        "RECOVERY_DESTINATION_CONFLICT" in warning
        for warning in resumed.warnings
    )


@pytest.mark.asyncio
async def test_historical_identity_is_revalidated_before_promotion(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    market_day = date(2026, 8, 11)
    original = save_valid_file(settings, market_day, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    original.path.unlink()
    real_authorize = repository.authorize_pending_repair_promotion

    def change_identity_before_authorization(*args, **kwargs):
        with sqlite3.connect(settings.state_db_path) as connection:
            connection.execute(
                """
                UPDATE date_sync_state SET csv_checksum_sha256 = ?
                WHERE market_date = ?
                """,
                ("f" * 64, market_day.isoformat()),
            )
        return real_authorize(*args, **kwargs)

    monkeypatch.setattr(
        repository,
        "authorize_pending_repair_promotion",
        change_identity_before_authorization,
    )
    client = FakeAsyncClient(
        {market_day.isoformat(): FetchResponse(200, fixture_bytes("valid_market.html"))}
    )

    with pytest.raises(StateDatabaseError, match="historical repair identity changed"):
        await reconcile_range(
            settings,
            repository,
            market_day.isoformat(),
            market_day.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )

    assert not original.path.exists()
    with sqlite3.connect(settings.state_db_path) as connection:
        candidate = connection.execute(
            """
            SELECT prior_checksum_sha256, disposition, promoted_at,
                   staged_relative_path
            FROM repair_candidates WHERE market_date = ?
            """,
            (market_day.isoformat(),),
        ).fetchone()
    assert candidate is not None
    assert candidate[0] == original.checksum
    assert candidate[1] == "PENDING_PROMOTION"
    assert candidate[2] is None
    assert (tmp_path / candidate[3]).exists()


def test_range_completeness_requires_every_date_resolved_and_artifact_healthy(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = reconciliation_settings(tmp_path, cooldown_seconds=0)
    repository = make_repository(settings, tmp_path)
    friday = date(2026, 8, 7)
    saturday = date(2026, 8, 8)
    sunday = date(2026, 8, 9)
    save_valid_file(settings, friday, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 8, 1, tzinfo=UTC),
    )
    record_empty_run(
        repository,
        saturday,
        datetime(2026, 8, 9, 2, tzinfo=UTC),
    )

    complete = plan(repository, settings, (saturday, friday))
    incomplete = plan(repository, settings, (sunday, saturday, friday))

    assert complete.complete
    assert complete.resolution_percentage == 100
    assert complete.verified_count == 1
    assert complete.confirmed_non_trading_count == 1
    assert incomplete.requested_dates == (
        friday.isoformat(),
        saturday.isoformat(),
        sunday.isoformat(),
    )
    assert not incomplete.complete
    assert incomplete.resolution_percentage == pytest.approx(200 / 3)
    assert incomplete.never_attempted_count == 1
