from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from psx_data_sync.client import PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.reconciliation import (
    ReconciliationPlanner,
    ReconciliationService,
    reconcile_range,
)
from psx_data_sync.state import (
    ClientFailureKind,
    FetchResponse,
    PersistentSyncStatus,
    ReconciliationAction,
    ReconciliationMode,
)
from psx_data_sync.state_db import StateRepository


UTC = timezone.utc
NOW = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
MARKET_DAY = date(2026, 8, 10)


def settings_for(
    tmp_path: Path,
    *,
    backoff_seconds: float = 0,
    max_rechecks: int = 2,
) -> Settings:
    return Settings(
        raw_output_dir=tmp_path / "raw",
        state_db_path=tmp_path / "state" / "psx_sync.db",
        repair_staging_dir=tmp_path / "state" / "repair_staging",
        retry_attempts=3,
        retry_backoff_initial_seconds=backoff_seconds,
        retry_backoff_max_seconds=backoff_seconds,
        retry_jitter_fraction=0,
        range_workers=1,
        max_rechecks_per_date_per_run=max_rechecks,
        reconciliation_cooldown_seconds=86_400,
    )


def repository_for(settings: Settings, tmp_path: Path) -> StateRepository:
    repository = StateRepository(settings.state_db_path, project_root=tmp_path)
    repository.initialize()
    return repository


class RetryableFailureClient:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, requested_date: date) -> FetchResponse:
        self.calls += 1
        raise PSXClientError(
            "fixture HTTP 503",
            kind=ClientFailureKind.HTTP,
            retryable=True,
            http_status=503,
            response_bytes=17,
        )


class ValidClient:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self, requested_date: date) -> FetchResponse:
        self.calls += 1
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


async def wait_for_attempt(repository: StateRepository) -> None:
    for _ in range(200):
        evidence = repository.get_attempt_evidence_for_range(
            MARKET_DAY.isoformat(), MARKET_DAY.isoformat()
        ).get(MARKET_DAY.isoformat())
        if evidence is not None and evidence.attempt_count:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("network attempt was not persisted")


@pytest.mark.asyncio
async def test_cancellation_during_retry_backoff_is_immediately_resumable(
    tmp_path: Path,
) -> None:
    settings = settings_for(tmp_path, backoff_seconds=60)
    repository = repository_for(settings, tmp_path)
    client = RetryableFailureClient()
    task = asyncio.create_task(
        reconcile_range(
            settings,
            repository,
            MARKET_DAY.isoformat(),
            MARKET_DAY.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )
    )
    await wait_for_attempt(repository)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    state = repository.get_date_state(MARKET_DAY)
    assert state is not None
    assert state.last_error_type == "CANCELLED"
    assert state.next_recheck_after is None
    resumed = ReconciliationPlanner(settings, repository, now=NOW).plan(
        (MARKET_DAY,),
        run_id="resume-plan",
        mode=ReconciliationMode.DRY_RUN,
        force_recheck=False,
    ).results[0]
    assert resumed.action_required is ReconciliationAction.NETWORK_RECHECK
    assert resumed.recheck_eligible_now
    assert resumed.next_recheck_after is None

    with sqlite3.connect(settings.state_db_path) as connection:
        parent = connection.execute(
            "SELECT status, network_recheck_count FROM reconciliation_runs"
        ).fetchone()
        child = connection.execute(
            "SELECT status, total_attempts FROM sync_runs"
        ).fetchone()
    assert parent == ("INTERRUPTED", 1)
    assert child == ("INTERRUPTED", 1)


@pytest.mark.asyncio
async def test_failed_apply_persists_partial_run_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path)
    repository = repository_for(settings, tmp_path)
    client = ValidClient()

    def fail_adjudication(*args, **kwargs) -> None:
        raise RuntimeError("injected adjudication failure")

    monkeypatch.setattr(
        ReconciliationService,
        "_record_and_adjudicate_network_result",
        fail_adjudication,
    )
    with pytest.raises(RuntimeError, match="injected adjudication failure"):
        await reconcile_range(
            settings,
            repository,
            MARKET_DAY.isoformat(),
            MARKET_DAY.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )

    with sqlite3.connect(settings.state_db_path) as connection:
        connection.row_factory = sqlite3.Row
        parent = connection.execute(
            "SELECT * FROM reconciliation_runs"
        ).fetchone()
        child = connection.execute("SELECT * FROM sync_runs").fetchone()
        attempts = connection.execute(
            "SELECT COUNT(*) FROM download_attempts"
        ).fetchone()[0]
        events = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_events"
        ).fetchone()[0]
    assert parent is not None
    assert parent["status"] == "FAILED"
    assert parent["network_recheck_planned_count"] == 1
    assert parent["network_recheck_count"] == 1
    assert parent["status_transition_count"] == events == 0
    assert parent["error_message"] == "injected adjudication failure"
    assert child is not None
    assert child["status"] == "INTERRUPTED"
    assert child["total_attempts"] == 1
    assert attempts == 1


@pytest.mark.asyncio
async def test_cancellation_drains_inflight_adjudication_before_terminalizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = settings_for(tmp_path, max_rechecks=1)
    repository = repository_for(settings, tmp_path)
    client = ValidClient()
    entered = threading.Event()
    release = threading.Event()
    original = ReconciliationService._record_and_adjudicate_network_result

    def blocked_adjudication(self, *args, **kwargs) -> None:
        entered.set()
        assert release.wait(timeout=2)
        original(self, *args, **kwargs)

    monkeypatch.setattr(
        ReconciliationService,
        "_record_and_adjudicate_network_result",
        blocked_adjudication,
    )
    task = asyncio.create_task(
        reconcile_range(
            settings,
            repository,
            MARKET_DAY.isoformat(),
            MARKET_DAY.isoformat(),
            apply=True,
            workers=1,
            client=client,
            now=NOW,
        )
    )
    assert await asyncio.to_thread(entered.wait, 2)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    with sqlite3.connect(settings.state_db_path) as connection:
        before_release = connection.execute(
            "SELECT status FROM reconciliation_runs"
        ).fetchone()[0]
        active_claims = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_recheck_claims"
        ).fetchone()[0]
    assert before_release == "RUNNING"
    assert active_claims == 1

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    canonical = (
        settings.raw_output_dir / f"market_{MARKET_DAY.isoformat()}.csv"
    )
    state = repository.get_date_state(MARKET_DAY)
    assert canonical.exists()
    assert state is not None
    assert state.status.value == "VERIFIED_TRADING_DATA"
    with sqlite3.connect(settings.state_db_path) as connection:
        terminal = connection.execute(
            """
            SELECT status, network_recheck_count, verified_count
            FROM reconciliation_runs
            """
        ).fetchone()
        candidate = connection.execute(
            "SELECT disposition FROM repair_candidates"
        ).fetchone()
        event_count = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_events"
        ).fetchone()[0]
        claims = connection.execute(
            "SELECT COUNT(*) FROM reconciliation_recheck_claims"
        ).fetchone()[0]
    assert terminal == ("INTERRUPTED", 1, 1)
    assert candidate == ("PROMOTED",)
    assert event_count >= 1
    assert claims == 0


def test_cancellation_marker_does_not_erase_conflict_diagnostics(
    tmp_path: Path,
) -> None:
    settings = settings_for(tmp_path)
    repository = repository_for(settings, tmp_path)
    repository.mark_artifact_issue(
        MARKET_DAY.isoformat(),
        PersistentSyncStatus.FILE_CONFLICT,
        error_type="HISTORICAL_MISMATCH",
        error_message="candidate differs from trusted identity",
    )

    changed = repository.mark_reconciliation_date_cancelled(
        MARKET_DAY.isoformat()
    )

    state = repository.get_date_state(MARKET_DAY)
    assert not changed
    assert state is not None
    assert state.status is PersistentSyncStatus.FILE_CONFLICT
    assert state.last_error_type == "HISTORICAL_MISMATCH"
