"""Bounded asynchronous orchestration for D2 date ranges."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from datetime import date, timedelta

from .client import AsyncPSXClient
from .config import MAX_RANGE_WORKERS, MIN_RANGE_WORKERS, Settings
from .downloader import AsyncSingleDateDownloader, validate_requested_date
from .state import (
    BenchmarkMetrics,
    DownloadAttemptEvent,
    DownloadResult,
    DownloadStatus,
    RangeDownloadResult,
)


logger = logging.getLogger(__name__)
ProgressCallback = Callable[[DownloadResult, int, int], None]
AsyncPreflight = Callable[[date], Awaitable[DownloadResult | None]]
AsyncAttemptObserver = Callable[[DownloadAttemptEvent], Awaitable[None]]
AsyncResultObserver = Callable[[DownloadResult], Awaitable[None]]

FAILURE_STATUSES = frozenset(
    {
        DownloadStatus.TEMPORARY_FAILURE,
        DownloadStatus.HTTP_FAILURE,
        DownloadStatus.PARSE_FAILURE,
        DownloadStatus.VALIDATION_FAILURE,
        DownloadStatus.EXISTING_FILE_INVALID,
        DownloadStatus.FILE_CONFLICT,
        DownloadStatus.SAVE_FAILURE,
        DownloadStatus.INVALID_DATE,
    }
)
UNRESOLVED_STATUSES = frozenset(
    {
        DownloadStatus.EMPTY_MARKET_RESPONSE,
        DownloadStatus.NON_TRADING_OR_EMPTY,
    }
)


def generate_date_range(
    start_date: str,
    end_date: str,
    *,
    today: date | None = None,
) -> tuple[date, ...]:
    """Return every calendar date in a validated inclusive range."""

    start = validate_requested_date(start_date, today=today)
    end = validate_requested_date(end_date, today=today)
    if start > end:
        raise ValueError("start date must be on or before end date")
    day_count = (end - start).days + 1
    return tuple(start + timedelta(days=offset) for offset in range(day_count))


def validate_workers(workers: int, settings: Settings) -> int:
    if isinstance(workers, bool) or workers < MIN_RANGE_WORKERS:
        raise ValueError(f"workers must be at least {MIN_RANGE_WORKERS}")
    effective_maximum = min(settings.max_range_workers, MAX_RANGE_WORKERS)
    if workers > effective_maximum:
        raise ValueError(
            f"workers cannot exceed the configured maximum of "
            f"{effective_maximum}"
        )
    return workers


def _unexpected_failure(requested_date: date, exc: Exception) -> DownloadResult:
    logger.exception(
        "unexpected isolated range failure date=%s", requested_date.isoformat()
    )
    return DownloadResult(
        requested_date=requested_date.isoformat(),
        status=DownloadStatus.TEMPORARY_FAILURE,
        error=f"unexpected per-date failure: {exc}",
    )


def build_range_result(
    requested_dates: Iterable[date],
    workers: int,
    total_duration_ms: float,
    results: Iterable[DownloadResult],
    *,
    warnings: Iterable[str] = (),
) -> RangeDownloadResult:
    """Aggregate lightweight per-date metadata in deterministic date order."""

    dates = tuple(sorted(set(requested_dates)))
    ordered_results = tuple(sorted(results, key=lambda item: item.requested_date))
    counts = Counter(result.status for result in ordered_results)
    counts_by_status = {
        status: counts.get(status, 0) for status in DownloadStatus
    }
    total_seconds = total_duration_ms / 1000
    result_count = len(ordered_results)
    network_fetched = sum(result.attempts > 0 for result in ordered_results)
    locally_skipped = sum(result.locally_skipped for result in ordered_results)
    verified_successful = sum(result.successful for result in ordered_results)
    total_valid_rows = sum(result.valid_row_count for result in ordered_results)
    failed_dates = tuple(
        result.requested_date
        for result in ordered_results
        if result.status in FAILURE_STATUSES
    )
    unresolved_dates = tuple(
        result.requested_date
        for result in ordered_results
        if result.status in UNRESOLVED_STATUSES
    )
    return RangeDownloadResult(
        start_date=dates[0].isoformat(),
        end_date=dates[-1].isoformat(),
        requested_dates=tuple(day.isoformat() for day in dates),
        workers=workers,
        total_duration_ms=total_duration_ms,
        results=ordered_results,
        counts_by_status=counts_by_status,
        total_parsed_rows=sum(
            result.parsed_row_count for result in ordered_results
        ),
        total_valid_rows=total_valid_rows,
        total_rejected_rows=sum(
            result.rejected_row_count for result in ordered_results
        ),
        total_response_bytes=sum(
            result.transferred_response_bytes for result in ordered_results
        ),
        total_retries=sum(
            max(result.attempts - 1, 0) for result in ordered_results
        ),
        rate_limit_occurrences=sum(
            result.rate_limit_count for result in ordered_results
        ),
        network_fetched_dates=network_fetched,
        locally_skipped_dates=locally_skipped,
        verified_successful_dates=verified_successful,
        failed_dates=failed_dates,
        unresolved_empty_dates=unresolved_dates,
        average_per_date_duration_ms=(
            sum(result.elapsed_ms for result in ordered_results) / result_count
            if result_count
            else 0.0
        ),
        dates_per_second=(result_count / total_seconds if total_seconds > 0 else 0.0),
        verified_dates_per_second=(
            verified_successful / total_seconds if total_seconds > 0 else 0.0
        ),
        network_dates_per_second=(
            network_fetched / total_seconds if total_seconds > 0 else 0.0
        ),
        rows_per_second=(
            total_valid_rows / total_seconds if total_seconds > 0 else 0.0
        ),
        warnings=tuple(warnings),
    )


class ConcurrentRangeDownloader:
    """Run a fixed number of async workers against one shared HTTP client."""

    def __init__(
        self,
        settings: Settings,
        client: AsyncPSXClient,
        *,
        workers: int,
        progress_callback: ProgressCallback | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
        preflight: AsyncPreflight | None = None,
        attempt_observer: AsyncAttemptObserver | None = None,
        result_observer: AsyncResultObserver | None = None,
    ) -> None:
        self.settings = settings
        self.client = client
        self.workers = validate_workers(workers, settings)
        self.progress_callback = progress_callback
        self.result_observer = result_observer
        self._date_downloader = AsyncSingleDateDownloader(
            settings,
            client,
            sleep=sleep,
            random_value=random_value,
            short_circuit_existing=True,
            preflight=preflight,
            attempt_observer=attempt_observer,
        )

    async def download_range(
        self,
        start_date: str,
        end_date: str,
    ) -> RangeDownloadResult:
        requested_dates = generate_date_range(start_date, end_date)
        return await self.download_dates(requested_dates)

    async def download_dates(
        self, requested_dates: Iterable[date]
    ) -> RangeDownloadResult:
        dates = tuple(sorted(set(requested_dates)))
        if not dates:
            raise ValueError("at least one requested date is required")
        for requested_date in dates:
            validate_requested_date(requested_date.isoformat())

        warnings: list[str] = []
        if len(dates) > self.settings.large_range_warning_days:
            warnings.append(
                f"large range contains {len(dates)} dates; full multi-year "
                "backfill and reconciliation belong to later milestones"
            )

        started = time.perf_counter()
        queue: asyncio.Queue[date | None] = asyncio.Queue(
            maxsize=max(self.workers * 2, 1)
        )
        outcomes: dict[str, DownloadResult] = {}
        completed = 0

        async def produce() -> None:
            for requested_date in dates:
                await queue.put(requested_date)
            for _ in range(self.workers):
                await queue.put(None)

        async def worker(worker_number: int) -> None:
            nonlocal completed
            while True:
                requested_date = await queue.get()
                if requested_date is None:
                    logger.info("range worker=%s stopped", worker_number)
                    return
                logger.info(
                    "range worker=%s date=%s started",
                    worker_number,
                    requested_date.isoformat(),
                )
                try:
                    outcome = await self._date_downloader.download(
                        requested_date.isoformat(),
                        worker_identifier=f"worker-{worker_number}",
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    outcome = _unexpected_failure(requested_date, exc)
                if self.result_observer is not None:
                    await self.result_observer(outcome)
                outcomes[requested_date.isoformat()] = outcome
                completed += 1
                logger.info(
                    "range worker=%s date=%s final_status=%s duration_ms=%.2f",
                    worker_number,
                    requested_date.isoformat(),
                    outcome.status.value,
                    outcome.elapsed_ms,
                )
                if self.progress_callback is not None:
                    try:
                        self.progress_callback(outcome, completed, len(dates))
                    except Exception:
                        logger.exception("range progress callback failed")

        tasks = [asyncio.create_task(produce(), name="psx-range-producer")]
        tasks.extend(
            asyncio.create_task(
                worker(worker_number),
                name=f"psx-range-worker-{worker_number}",
            )
            for worker_number in range(1, self.workers + 1)
        )
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        total_duration_ms = (time.perf_counter() - started) * 1000
        return build_range_result(
            dates,
            self.workers,
            total_duration_ms,
            outcomes.values(),
            warnings=warnings,
        )


async def fetch_date_range(
    settings: Settings,
    start_date: str,
    end_date: str,
    *,
    workers: int | None = None,
    progress_callback: ProgressCallback | None = None,
    client: AsyncPSXClient | None = None,
    preflight: AsyncPreflight | None = None,
    attempt_observer: AsyncAttemptObserver | None = None,
    result_observer: AsyncResultObserver | None = None,
) -> RangeDownloadResult:
    """Own the pooled client lifetime, including cancellation cleanup."""

    resolved_workers = validate_workers(
        settings.range_workers if workers is None else workers,
        settings,
    )
    requested_dates = generate_date_range(start_date, end_date)
    pooled_client = client or AsyncPSXClient(
        settings,
        workers=resolved_workers,
    )
    async with pooled_client:
        downloader = ConcurrentRangeDownloader(
            settings,
            pooled_client,
            workers=resolved_workers,
            progress_callback=progress_callback,
            preflight=preflight,
            attempt_observer=attempt_observer,
            result_observer=result_observer,
        )
        return await downloader.download_dates(requested_dates)


def compare_benchmark_results(
    results: Iterable[RangeDownloadResult],
) -> tuple[BenchmarkMetrics, ...]:
    """Build a comparable workers=1/2/4-style benchmark table from full runs."""

    materialized = tuple(results)
    if not materialized:
        return ()
    requested_dates = materialized[0].requested_dates
    if any(result.requested_dates != requested_dates for result in materialized[1:]):
        raise ValueError("benchmark results must cover the same requested dates")
    worker_counts = [result.workers for result in materialized]
    if len(set(worker_counts)) != len(worker_counts):
        raise ValueError("benchmark results must use distinct worker counts")

    metrics = (
        BenchmarkMetrics(
            workers=result.workers,
            requested_dates=result.requested_count,
            total_duration_ms=result.total_duration_ms,
            dates_per_second=result.dates_per_second,
            verified_dates_per_second=result.verified_dates_per_second,
            network_dates_per_second=result.network_dates_per_second,
            rows_per_second=result.rows_per_second,
            retries=result.total_retries,
            failures=len(result.failed_dates),
            response_bytes=result.total_response_bytes,
        )
        for result in materialized
    )
    return tuple(sorted(metrics, key=lambda item: item.workers))


async def benchmark_worker_counts(
    run_for_workers: Callable[[int], Awaitable[RangeDownloadResult]],
    worker_counts: tuple[int, ...] = (1, 2, 4),
) -> tuple[BenchmarkMetrics, ...]:
    """Run the same caller-defined small range at conservative worker counts.

    The caller controls isolated output directories or equivalent fixture state,
    preventing earlier benchmark runs from turning later ones into local skips.
    """

    completed: list[RangeDownloadResult] = []
    for workers in worker_counts:
        if workers < MIN_RANGE_WORKERS or workers > MAX_RANGE_WORKERS:
            raise ValueError(
                f"benchmark workers must be between 1 and {MAX_RANGE_WORKERS}"
            )
        completed.append(await run_for_workers(workers))
    return compare_benchmark_results(completed)
