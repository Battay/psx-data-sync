from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import date, timedelta
from pathlib import Path

import pytest

from psx_data_sync.config import Settings
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import DownloadStatus, FetchResponse, PersistentSyncStatus
from psx_data_sync.state_db import AsyncStateRepository, StateRepository
from psx_data_sync.synchronizer import ConcurrentRangeDownloader
from psx_data_sync.validator import validate_rows


class ConcurrentFakeClient:
    def __init__(self, outcomes, *, delay: float = 0.0) -> None:
        self.outcomes = {key: deque(values) for key, values in outcomes.items()}
        self.delay = delay
        self.calls = defaultdict(int)
        self.active = 0
        self.max_active = 0

    async def fetch(self, requested_date: date) -> FetchResponse:
        date_text = requested_date.isoformat()
        self.calls[date_text] += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            outcome = self.outcomes[date_text].popleft()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        finally:
            self.active -= 1


class BlockingAfterFirstClient:
    def __init__(self, response: FetchResponse) -> None:
        self.response = response
        self.calls = defaultdict(int)
        self.total_calls = 0
        self.blocked = asyncio.Event()

    async def fetch(self, requested_date: date) -> FetchResponse:
        date_text = requested_date.isoformat()
        self.calls[date_text] += 1
        self.total_calls += 1
        if self.total_calls == 1:
            return self.response
        self.blocked.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def stateful_settings(tmp_path: Path) -> Settings:
    return Settings(
        raw_output_dir=tmp_path / "raw",
        state_db_path=tmp_path / "state" / "psx_sync.db",
        retry_attempts=1,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
        retry_jitter_fraction=0,
    )


def make_repository(settings: Settings, tmp_path: Path) -> StateRepository:
    repository = StateRepository(settings.state_db_path, project_root=tmp_path)
    repository.initialize()
    return repository


def valid_response(fixture_bytes) -> FetchResponse:
    return FetchResponse(200, fixture_bytes("valid_market.html"))


def save_valid(output_dir: Path, market_date: date, fixture_bytes) -> None:
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    save_canonical_csv(rows, market_date, output_dir)


async def run_stateful_range(
    repository: StateRepository,
    settings: Settings,
    client,
    requested_dates: tuple[date, ...],
    *,
    workers: int,
    run_id: str,
):
    state = AsyncStateRepository(repository)
    downloader = ConcurrentRangeDownloader(
        settings,
        client,
        workers=workers,
        preflight=lambda day: state.prepare_fetch(
            day, settings.raw_output_dir, settings.canonical_columns
        ),
        attempt_observer=lambda event: state.record_attempt(run_id, event),
        result_observer=lambda result: state.record_download_result(run_id, result),
    )
    return await downloader.download_dates(requested_dates)


@pytest.mark.asyncio
async def test_mixed_state_range_fetches_only_dates_that_need_network(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = stateful_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    days = (date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5))
    save_valid(settings.raw_output_dir, days[0], fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    response = valid_response(fixture_bytes)
    client = ConcurrentFakeClient(
        {days[1].isoformat(): [response], days[2].isoformat(): [response]},
        delay=0.01,
    )
    run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), 3, 2
    )

    result = await run_stateful_range(
        repository, settings, client, days, workers=2, run_id=run_id
    )
    run = repository.finish_sync_run(run_id)

    assert client.calls[days[0].isoformat()] == 0
    assert client.calls[days[1].isoformat()] == 1
    assert client.calls[days[2].isoformat()] == 1
    assert client.max_active == 2
    assert result.network_fetched_dates == 2
    assert result.locally_skipped_dates == 1
    assert tuple(item.requested_date for item in result.results) == tuple(
        day.isoformat() for day in days
    )
    assert run.completed_count == 3
    assert run.network_fetch_count == 2
    assert run.local_skip_count == 1
    assert run.total_attempts == 2
    assert all(
        repository.get_date_state(day).status
        is PersistentSyncStatus.VERIFIED_TRADING_DATA
        for day in days
    )


@pytest.mark.asyncio
async def test_all_verified_range_performs_zero_network_calls(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = stateful_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    days = (date(2026, 8, 4), date(2026, 8, 5))
    for day in days:
        save_valid(settings.raw_output_dir, day, fixture_bytes)
    repository.bootstrap_local_files(settings.raw_output_dir)
    client = ConcurrentFakeClient({})
    run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), 2, 2
    )

    result = await run_stateful_range(
        repository, settings, client, days, workers=2, run_id=run_id
    )
    run = repository.finish_sync_run(run_id)

    assert not client.calls
    assert result.network_fetched_dates == 0
    assert result.locally_skipped_dates == 2
    assert run.network_fetch_count == 0
    assert run.total_attempts == 0
    assert run.local_skip_count == 2


@pytest.mark.asyncio
async def test_interrupted_range_resumes_without_redownloading_completed_date(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = stateful_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    days = (date(2026, 8, 4), date(2026, 8, 5))
    response = valid_response(fixture_bytes)
    interrupted_client = BlockingAfterFirstClient(response)
    first_run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), 2, 1
    )
    task = asyncio.create_task(
        run_stateful_range(
            repository,
            settings,
            interrupted_client,
            days,
            workers=1,
            run_id=first_run_id,
        )
    )
    await asyncio.wait_for(interrupted_client.blocked.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    interrupted_run = repository.finish_sync_run(first_run_id, interrupted=True)

    resumed_client = ConcurrentFakeClient({days[1].isoformat(): [response]})
    second_run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), 2, 1
    )
    resumed = await run_stateful_range(
        repository,
        settings,
        resumed_client,
        days,
        workers=1,
        run_id=second_run_id,
    )
    resumed_run = repository.finish_sync_run(second_run_id)

    assert interrupted_run.status.value == "INTERRUPTED"
    assert interrupted_run.completed_count == 1
    assert interrupted_run.total_attempts == 2
    assert resumed_client.calls[days[0].isoformat()] == 0
    assert resumed_client.calls[days[1].isoformat()] == 1
    assert resumed.locally_skipped_dates == 1
    assert resumed.network_fetched_dates == 1
    assert resumed_run.completed_count == 2
    assert resumed_run.local_skip_count == 1
    assert resumed_run.network_fetch_count == 1
    assert repository.get_date_state(
        days[1]
    ).status is PersistentSyncStatus.VERIFIED_TRADING_DATA


@pytest.mark.asyncio
async def test_many_concurrent_dates_serialize_db_writes_not_network(
    tmp_path: Path, fixture_bytes
) -> None:
    settings = stateful_settings(tmp_path)
    repository = make_repository(settings, tmp_path)
    days = tuple(date(2020, 1, 1) + timedelta(days=offset) for offset in range(20))
    response = valid_response(fixture_bytes)
    client = ConcurrentFakeClient(
        {day.isoformat(): [response] for day in days}, delay=0.01
    )
    run_id = repository.begin_sync_run(
        "fetch-range", days[0].isoformat(), days[-1].isoformat(), len(days), 4
    )

    result = await run_stateful_range(
        repository, settings, client, days, workers=4, run_id=run_id
    )
    run = repository.finish_sync_run(run_id)

    assert client.max_active == 4
    assert result.network_fetched_dates == 20
    assert result.verified_successful_dates == 20
    assert run.completed_count == 20
    assert run.total_attempts == 20
    assert len(repository.list_dates_by_status(
        [PersistentSyncStatus.VERIFIED_TRADING_DATA]
    )) == 20
    assert all(len(repository.get_recent_attempts(day.isoformat())) == 1 for day in days)
