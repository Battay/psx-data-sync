from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import date
from pathlib import Path

import pytest

import psx_data_sync.downloader as downloader_module
from psx_data_sync.client import PSXClientError
from psx_data_sync.config import Settings
from psx_data_sync.downloader import AsyncSingleDateDownloader
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import (
    ClientFailureKind,
    DownloadResult,
    DownloadStatus,
    FetchResponse,
)
from psx_data_sync.synchronizer import (
    ConcurrentRangeDownloader,
    benchmark_worker_counts,
    build_range_result,
    compare_benchmark_results,
    fetch_date_range,
    generate_date_range,
    validate_workers,
)
from psx_data_sync.validator import validate_rows


def response(content: bytes) -> FetchResponse:
    return FetchResponse(status_code=200, content=content)


def range_settings(
    tmp_path: Path,
    *,
    attempts: int = 2,
    warning_days: int = 365,
) -> Settings:
    return Settings(
        raw_output_dir=tmp_path,
        retry_attempts=attempts,
        retry_backoff_initial_seconds=0,
        retry_backoff_max_seconds=0,
        large_range_warning_days=warning_days,
    )


class FakeAsyncClient:
    def __init__(self, outcomes, *, delays=None) -> None:
        self.outcomes = {
            key: deque(value) for key, value in outcomes.items()
        }
        self.delays = delays or {}
        self.calls = defaultdict(int)
        self.active = 0
        self.max_active = 0
        self.closed = False

    async def fetch(self, requested_date: date) -> FetchResponse:
        iso_date = requested_date.isoformat()
        self.calls[iso_date] += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delays.get(iso_date, 0))
            outcome = self.outcomes[iso_date].popleft()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        finally:
            self.active -= 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self.closed = True


def test_inclusive_date_generation_keeps_weekends_and_single_days() -> None:
    dates = generate_date_range(
        "2026-08-01", "2026-08-05", today=date(2026, 8, 19)
    )

    assert tuple(item.isoformat() for item in dates) == (
        "2026-08-01",
        "2026-08-02",
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    )
    assert generate_date_range(
        "2026-08-05", "2026-08-05", today=date(2026, 8, 19)
    ) == (date(2026, 8, 5),)


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2026-08-05", "2026-08-01", "start date"),
        ("2026-8-1", "2026-08-05", "YYYY-MM-DD"),
        ("2026-02-30", "2026-08-05", "invalid calendar"),
        ("2999-01-01", "2999-01-02", "future"),
    ],
)
def test_invalid_ranges_are_rejected(start: str, end: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        generate_date_range(start, end, today=date(2026, 8, 19))


def test_worker_bounds_are_enforced(tmp_path: Path) -> None:
    settings = range_settings(tmp_path)

    assert validate_workers(1, settings) == 1
    assert validate_workers(16, settings) == 16
    with pytest.raises(ValueError, match="at least"):
        validate_workers(0, settings)
    with pytest.raises(ValueError, match="cannot exceed"):
        validate_workers(17, settings)


@pytest.mark.asyncio
async def test_worker_limit_and_deterministic_result_order(
    tmp_path: Path, fixture_bytes
) -> None:
    content = response(fixture_bytes("valid_market.html"))
    dates = generate_date_range(
        "2026-08-01", "2026-08-05", today=date(2026, 8, 19)
    )
    outcomes = {day.isoformat(): [content] for day in dates}
    delays = {
        "2026-08-01": 0.04,
        "2026-08-02": 0.03,
        "2026-08-03": 0.02,
        "2026-08-04": 0.01,
        "2026-08-05": 0,
    }
    client = FakeAsyncClient(outcomes, delays=delays)
    downloader = ConcurrentRangeDownloader(
        range_settings(tmp_path), client, workers=2
    )

    result = await downloader.download_dates(reversed(dates))

    assert client.max_active == 2
    assert tuple(item.requested_date for item in result.results) == tuple(
        day.isoformat() for day in dates
    )
    assert result.counts_by_status[DownloadStatus.TRADING_DATA] == 5


@pytest.mark.asyncio
async def test_mixed_outcomes_are_isolated_and_aggregated(
    tmp_path: Path, fixture_bytes
) -> None:
    valid = response(fixture_bytes("valid_market.html"))
    empty = response(fixture_bytes("empty_shell.html"))
    server_error = PSXClientError(
        "HTTP 500",
        kind=ClientFailureKind.HTTP,
        retryable=True,
        http_status=500,
        response_bytes=5,
    )
    timeout = PSXClientError(
        "timeout",
        kind=ClientFailureKind.TIMEOUT,
        retryable=True,
    )
    rows = validate_rows(parse_equity_rows(valid.content)).valid_rows
    save_canonical_csv(rows, date(2026, 8, 5), tmp_path)
    outcomes = {
        "2026-08-01": [valid],
        "2026-08-02": [empty, valid],
        "2026-08-03": [server_error, valid],
        "2026-08-04": [timeout, timeout],
        "2026-08-05": [],
    }
    progress: list[tuple[str, int]] = []
    client = FakeAsyncClient(outcomes)
    downloader = ConcurrentRangeDownloader(
        range_settings(tmp_path),
        client,
        workers=3,
        progress_callback=lambda item, completed, _: progress.append(
            (item.requested_date, completed)
        ),
    )

    result = await downloader.download_range("2026-08-01", "2026-08-05")

    assert [item.status for item in result.results] == [
        DownloadStatus.TRADING_DATA,
        DownloadStatus.TRADING_DATA,
        DownloadStatus.TRADING_DATA,
        DownloadStatus.TEMPORARY_FAILURE,
        DownloadStatus.ALREADY_PRESENT,
    ]
    assert client.calls["2026-08-05"] == 0
    assert result.network_fetched_dates == 4
    assert result.locally_skipped_dates == 1
    assert result.verified_successful_dates == 4
    assert result.total_parsed_rows == 12
    assert result.total_valid_rows == 12
    assert result.total_rejected_rows == 0
    assert result.total_retries == 3
    assert result.total_response_bytes == (
        len(valid.content) * 3 + len(empty.content) + 5
    )
    assert result.failed_dates == ("2026-08-04",)
    assert not result.unresolved_empty_dates
    assert len(progress) == 5
    assert all(
        (tmp_path / f"market_2026-08-0{day}.csv").exists()
        for day in (1, 2, 3, 5)
    )
    assert not (tmp_path / "market_2026-08-04.csv").exists()
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.asyncio
async def test_rate_limit_respects_retry_after_with_async_sleep(
    tmp_path: Path, fixture_bytes, monkeypatch
) -> None:
    rate_limit = PSXClientError(
        "HTTP 429",
        kind=ClientFailureKind.HTTP,
        retryable=True,
        http_status=429,
        retry_after_seconds=3.0,
    )
    client = FakeAsyncClient(
        {"2026-08-05": [rate_limit, response(fixture_bytes("valid_market.html"))]}
    )
    delays: list[float] = []

    async def async_sleep(delay: float) -> None:
        delays.append(delay)

    def blocking_sleep(_: float) -> None:
        raise AssertionError("blocking time.sleep was called")

    monkeypatch.setattr(downloader_module.time, "sleep", blocking_sleep)
    downloader = AsyncSingleDateDownloader(
        range_settings(tmp_path),
        client,
        sleep=async_sleep,
        random_value=lambda: 0,
    )

    result = await downloader.download("2026-08-05")

    assert result.status is DownloadStatus.TRADING_DATA
    assert result.attempts == 2
    assert result.rate_limit_count == 1
    assert delays == [3.0]


@pytest.mark.asyncio
async def test_malformed_response_fails_one_date_without_a_file(
    tmp_path: Path, fixture_bytes
) -> None:
    malformed = response(fixture_bytes("malformed.html"))
    client = FakeAsyncClient({"2026-08-05": [malformed, malformed]})

    result = await ConcurrentRangeDownloader(
        range_settings(tmp_path), client, workers=1
    ).download_range("2026-08-05", "2026-08-05")

    assert result.results[0].status is DownloadStatus.PARSE_FAILURE
    assert result.failed_dates == ("2026-08-05",)
    assert not list(tmp_path.glob("*.csv"))


@pytest.mark.asyncio
async def test_duplicate_dates_are_scheduled_once(tmp_path: Path, fixture_bytes) -> None:
    requested = date(2026, 8, 5)
    client = FakeAsyncClient(
        {requested.isoformat(): [response(fixture_bytes("valid_market.html"))]}
    )

    result = await ConcurrentRangeDownloader(
        range_settings(tmp_path), client, workers=2
    ).download_dates([requested, requested, requested])

    assert result.requested_count == 1
    assert client.calls[requested.isoformat()] == 1


@pytest.mark.asyncio
async def test_large_range_gets_noninteractive_warning(
    tmp_path: Path, fixture_bytes
) -> None:
    dates = (date(2026, 8, 4), date(2026, 8, 5))
    valid = response(fixture_bytes("valid_market.html"))
    client = FakeAsyncClient({day.isoformat(): [valid] for day in dates})

    result = await ConcurrentRangeDownloader(
        range_settings(tmp_path, warning_days=1), client, workers=1
    ).download_dates(dates)

    assert "large range" in result.warnings[0]


class BlockingContextClient:
    def __init__(self, first_response: FetchResponse | None = None) -> None:
        self.first_response = first_response
        self.calls = 0
        self.blocked = asyncio.Event()
        self.closed = False

    async def fetch(self, _: date) -> FetchResponse:
        self.calls += 1
        if self.calls == 1 and self.first_response is not None:
            return self.first_response
        self.blocked.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self.closed = True


@pytest.mark.asyncio
async def test_cancellation_closes_client_and_keeps_completed_file(
    tmp_path: Path, fixture_bytes
) -> None:
    client = BlockingContextClient(response(fixture_bytes("valid_market.html")))
    task = asyncio.create_task(
        fetch_date_range(
            range_settings(tmp_path),
            "2026-08-04",
            "2026-08-05",
            workers=1,
            client=client,
        )
    )
    await asyncio.wait_for(client.blocked.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    completed = tmp_path / "market_2026-08-04.csv"
    assert client.closed
    assert completed.exists()
    assert completed.read_bytes().startswith(b"symbol,ldcp,open")
    assert not list(tmp_path.glob("*.tmp"))


def test_benchmark_comparison_keeps_failures_and_sorts_workers() -> None:
    dates = (date(2026, 8, 4), date(2026, 8, 5))
    outcomes = (
        DownloadResult("2026-08-04", DownloadStatus.TRADING_DATA, valid_row_count=3),
        DownloadResult("2026-08-05", DownloadStatus.TEMPORARY_FAILURE, attempts=2),
    )
    workers_four = build_range_result(dates, 4, 1000, outcomes)
    workers_one = build_range_result(dates, 1, 2000, outcomes)

    comparison = compare_benchmark_results([workers_four, workers_one])

    assert [item.workers for item in comparison] == [1, 4]
    assert comparison[0].failures == 1
    assert comparison[0].requested_dates == 2
    assert comparison[1].dates_per_second == 2


@pytest.mark.asyncio
async def test_benchmark_helper_runs_default_worker_comparison() -> None:
    dates = (date(2026, 8, 5),)
    called: list[int] = []

    async def run(workers: int):
        called.append(workers)
        return build_range_result(
            dates,
            workers,
            1000 / workers,
            [DownloadResult("2026-08-05", DownloadStatus.TRADING_DATA)],
        )

    metrics = await benchmark_worker_counts(run)

    assert called == [1, 2, 4]
    assert [item.workers for item in metrics] == [1, 2, 4]
