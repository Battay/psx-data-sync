"""Typer command-line interface for PSX Data Sync."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from .client import PSXClient
from .config import MAX_RANGE_WORKERS, Settings
from .downloader import SingleDateDownloader, validate_requested_date
from .state import (
    DownloadResult,
    DownloadStatus,
    PersistentSyncStatus,
    RangeDownloadResult,
    StateSummary,
)
from .state_db import AsyncStateRepository, StateDatabaseError, StateRepository
from .synchronizer import (
    fetch_date_range,
    generate_date_range,
    validate_workers,
)


app = typer.Typer(
    name="psx-data-sync",
    help="Reliable Pakistan Stock Exchange historical data downloads.",
    no_args_is_help=True,
)
console = Console()
logger = logging.getLogger(__name__)


@app.callback()
def main() -> None:
    """Download and validate PSX historical market data."""


def run_download(requested_date: str) -> DownloadResult:
    try:
        parsed_date = validate_requested_date(requested_date)
    except ValueError as exc:
        return DownloadResult(
            requested_date=requested_date,
            status=DownloadStatus.INVALID_DATE,
            error=str(exc),
        )

    settings = Settings.from_env()
    repository = StateRepository(
        settings.state_db_path,
        source_endpoint=settings.historical_url,
    )
    repository.initialize()
    run_id = repository.begin_sync_run(
        "fetch",
        parsed_date.isoformat(),
        parsed_date.isoformat(),
        1,
        1,
    )
    started = time.perf_counter()
    try:
        with PSXClient(settings) as client:
            downloader = SingleDateDownloader(
                settings,
                client,
                preflight=lambda day: repository.prepare_fetch(
                    day,
                    settings.raw_output_dir,
                    settings.canonical_columns,
                ),
                attempt_observer=lambda event: repository.record_attempt(
                    run_id, event
                ),
            )
            result = downloader.download(
                requested_date, worker_identifier="single-date"
            )
        repository.record_download_result(run_id, result)
        repository.finish_sync_run(
            run_id,
            duration_ms=(time.perf_counter() - started) * 1000,
        )
        return result
    except BaseException:
        try:
            repository.finish_sync_run(
                run_id,
                interrupted=True,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception:
            logger.exception("failed to mark single-date sync run interrupted")
        raise


def run_range_download(
    start_date: str,
    end_date: str,
    workers: int,
    *,
    settings: Settings | None = None,
    progress_callback=None,
) -> RangeDownloadResult:
    resolved_settings = settings or Settings.from_env()
    requested_dates = generate_date_range(start_date, end_date)
    repository = StateRepository(
        resolved_settings.state_db_path,
        source_endpoint=resolved_settings.historical_url,
    )
    repository.initialize()
    run_id = repository.begin_sync_run(
        "fetch-range",
        start_date,
        end_date,
        len(requested_dates),
        workers,
    )
    started = time.perf_counter()

    async def execute() -> RangeDownloadResult:
        state = AsyncStateRepository(repository)
        return await fetch_date_range(
            resolved_settings,
            start_date,
            end_date,
            workers=workers,
            progress_callback=progress_callback,
            preflight=lambda day: state.prepare_fetch(
                day,
                resolved_settings.raw_output_dir,
                resolved_settings.canonical_columns,
            ),
            attempt_observer=lambda event: state.record_attempt(run_id, event),
            result_observer=lambda result: state.record_download_result(
                run_id, result
            ),
        )

    try:
        result = asyncio.run(execute())
        repository.finish_sync_run(
            run_id,
            duration_ms=(time.perf_counter() - started) * 1000,
        )
        return replace(result, run_id=run_id)
    except BaseException:
        try:
            repository.finish_sync_run(
                run_id,
                interrupted=True,
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception:
            logger.exception("failed to mark range sync run interrupted")
        raise


def _human_bytes(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    return f"{size / 1024:.1f} KB"


def render_result(result: DownloadResult) -> None:
    console.print("[bold]PSX Data Sync — Single Date Fetch[/bold]")
    console.print()
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Date:", result.requested_date)
    table.add_row("Status:", result.status.value)
    table.add_row("HTTP:", str(result.http_status) if result.http_status else "—")
    table.add_row("Attempts:", str(result.attempts))
    table.add_row("Response:", _human_bytes(result.response_bytes))
    table.add_row("Parsed rows:", str(result.parsed_row_count))
    table.add_row("Valid rows:", str(result.valid_row_count))
    table.add_row("Rejected rows:", str(result.rejected_row_count))
    table.add_row("Duration:", f"{result.elapsed_ms / 1000:.2f} s")
    table.add_row(
        "Timing:",
        (
            f"network {result.network_ms:.1f} ms; parse {result.parse_ms:.1f} ms; "
            f"validation {result.validation_ms:.1f} ms; save {result.save_ms:.1f} ms"
        ),
    )
    table.add_row("Saved:", str(result.saved_path) if result.saved_path else "—")
    table.add_row("SHA-256:", result.checksum or "—")
    console.print(table)
    if result.warnings:
        console.print("\n[bold yellow]Warnings:[/bold yellow]")
        for warning in result.warnings:
            console.print(f"• {warning}")
    if result.error:
        console.print(f"\n[bold red]Error:[/bold red] {result.error}")


def render_range_result(result: RangeDownloadResult, output_dir: Path) -> None:
    """Render deterministic aggregate and per-date range outcomes."""

    counts = result.counts_by_status
    empty_count = sum(
        counts.get(status, 0)
        for status in (
            DownloadStatus.EMPTY_MARKET_RESPONSE,
            DownloadStatus.NON_TRADING_OR_EMPTY,
        )
    )
    console.print("[bold]PSX Data Sync — Range Fetch[/bold]")
    console.print(f"{result.start_date} → {result.end_date}")
    console.print(f"Workers: {result.workers}\n")

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="bold")
    summary.add_column(justify="right")
    summary.add_row("Requested dates:", f"{result.requested_count:,}")
    summary.add_row(
        "Trading data:",
        f"{counts.get(DownloadStatus.TRADING_DATA, 0):,}",
    )
    summary.add_row(
        "Already present:",
        f"{counts.get(DownloadStatus.ALREADY_PRESENT, 0):,}",
    )
    summary.add_row("Empty/unresolved:", f"{empty_count:,}")
    summary.add_row("Failures:", f"{len(result.failed_dates):,}")
    summary.add_row("Network-fetched dates:", f"{result.network_fetched_dates:,}")
    summary.add_row("Locally skipped dates:", f"{result.locally_skipped_dates:,}")
    summary.add_row("Parsed rows:", f"{result.total_parsed_rows:,}")
    summary.add_row("Valid rows:", f"{result.total_valid_rows:,}")
    summary.add_row("Rejected rows:", f"{result.total_rejected_rows:,}")
    summary.add_row("Retries:", f"{result.total_retries:,}")
    summary.add_row("Rate limits:", f"{result.rate_limit_occurrences:,}")
    summary.add_row("Response bytes:", _human_bytes(result.total_response_bytes))
    summary.add_row("Duration:", f"{result.total_duration_ms / 1000:.2f} s")
    summary.add_row("Throughput:", f"{result.dates_per_second:.2f} dates/s")
    summary.add_row(
        "Verified throughput:",
        f"{result.verified_dates_per_second:.2f} dates/s",
    )
    summary.add_row(
        "Network throughput:",
        f"{result.network_dates_per_second:.2f} dates/s",
    )
    summary.add_row("Rows throughput:", f"{result.rows_per_second:,.1f} rows/s")
    summary.add_row("Output:", str(output_dir))
    if result.run_id:
        summary.add_row("Run ID:", result.run_id)
    console.print(summary)

    outcomes = Table(title="Per-date outcomes", show_lines=False)
    outcomes.add_column("Date")
    outcomes.add_column("Status", no_wrap=True)
    outcomes.add_column("Attempts", justify="right")
    outcomes.add_column("Valid rows", justify="right")
    outcomes.add_column("Detail")
    for item in result.results:
        detail = item.error or (
            "local" if item.locally_skipped else "; ".join(item.warnings[:1])
        )
        outcomes.add_row(
            item.requested_date,
            item.status.value,
            str(item.attempts),
            f"{item.valid_row_count:,}",
            detail,
        )
    console.print("\n", outcomes)

    if result.failed_dates:
        console.print(
            "[bold red]Failed dates:[/bold red] " + ", ".join(result.failed_dates)
        )
    if result.unresolved_empty_dates:
        console.print(
            "[bold yellow]Empty/unresolved dates:[/bold yellow] "
            + ", ".join(result.unresolved_empty_dates)
        )
    for warning in result.warnings:
        console.print(f"[yellow]Warning:[/yellow] {warning}")


@app.command("fetch")
def fetch_command(
    requested_date: Annotated[
        str,
        typer.Option(
            "--date",
            "-d",
            help="One historical date in YYYY-MM-DD format.",
            metavar="YYYY-MM-DD",
        ),
    ],
) -> None:
    """Fetch, validate, and save one historical market date."""

    try:
        result = run_download(requested_date)
    except (OSError, ValueError, sqlite3.Error, StateDatabaseError) as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc
    render_result(result)
    if result.successful:
        return
    if result.status is DownloadStatus.INVALID_DATE:
        raise typer.Exit(code=2)
    if result.status in {
        DownloadStatus.NON_TRADING_OR_EMPTY,
        DownloadStatus.EMPTY_MARKET_RESPONSE,
    }:
        raise typer.Exit(code=3)
    raise typer.Exit(code=1)


@app.command("fetch-range")
def fetch_range_command(
    start_date: Annotated[
        str,
        typer.Option(
            "--start",
            "-s",
            help="Inclusive first date in YYYY-MM-DD format.",
            metavar="YYYY-MM-DD",
        ),
    ],
    end_date: Annotated[
        str,
        typer.Option(
            "--end",
            "-e",
            help="Inclusive final date in YYYY-MM-DD format.",
            metavar="YYYY-MM-DD",
        ),
    ],
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help=(
                f"Concurrent requests (1–{MAX_RANGE_WORKERS}; default 4, "
                "overridable by PSX_RANGE_WORKERS)."
            ),
            min=1,
            max=MAX_RANGE_WORKERS,
        ),
    ] = None,
) -> None:
    """Fetch an inclusive date range with bounded asynchronous concurrency."""

    try:
        settings = Settings.from_env()
    except ValueError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    try:
        resolved_workers = validate_workers(
            settings.range_workers if workers is None else workers,
            settings,
        )
        requested_dates = generate_date_range(start_date, end_date)
    except ValueError as exc:
        console.print(f"[bold red]Input error:[/bold red] {exc}")
        raise typer.Exit(code=2) from exc

    if len(requested_dates) > settings.large_range_warning_days:
        console.print(
            f"[yellow]Warning:[/yellow] requested range contains "
            f"{len(requested_dates)} dates; large backfills belong to later "
            "synchronization milestones."
        )

    progress_started = time.perf_counter()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TextColumn("{task.fields[throughput]}"),
        console=console,
        transient=True,
    ) as progress:
        progress_task = progress.add_task(
            "Fetching dates",
            total=len(requested_dates),
            throughput="0.00 dates/s",
        )

        def update_progress(
            _: DownloadResult, completed: int, __: int
        ) -> None:
            elapsed = max(time.perf_counter() - progress_started, 1e-9)
            progress.update(
                progress_task,
                completed=completed,
                throughput=f"{completed / elapsed:.2f} dates/s",
            )

        try:
            result = run_range_download(
                start_date,
                end_date,
                resolved_workers,
                settings=settings,
                progress_callback=update_progress,
            )
        except KeyboardInterrupt as exc:
            console.print(
                "[bold yellow]Range fetch interrupted.[/bold yellow] Completed "
                "atomic files remain valid; the shared HTTP client was closed."
            )
            raise typer.Exit(code=130) from exc
        except (OSError, ValueError, sqlite3.Error, StateDatabaseError) as exc:
            console.print(f"[bold red]Range error:[/bold red] {exc}")
            raise typer.Exit(code=1) from exc

    render_range_result(result, settings.raw_output_dir)
    if result.has_failures:
        raise typer.Exit(code=1)
    if result.has_unresolved_empty:
        raise typer.Exit(code=3)


def _repository_from_settings(settings: Settings) -> StateRepository:
    repository = StateRepository(
        settings.state_db_path,
        source_endpoint=settings.historical_url,
    )
    repository.initialize()
    return repository


@app.command("state-bootstrap")
def state_bootstrap_command() -> None:
    """Index existing canonical CSV files without making network requests."""

    try:
        settings = Settings.from_env()
        repository = _repository_from_settings(settings)
        result = repository.bootstrap_local_files(settings.raw_output_dir)
    except (OSError, ValueError, sqlite3.Error, StateDatabaseError) as exc:
        console.print(f"[bold red]Bootstrap error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    console.print("[bold]PSX Data Sync — Local State Bootstrap[/bold]\n")
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column(justify="right")
    table.add_row("Database:", str(settings.state_db_path))
    table.add_row("Discovered files:", str(result.discovered_files))
    table.add_row("Newly indexed:", str(result.indexed_files))
    table.add_row("Already indexed:", str(result.unchanged_files))
    table.add_row("Invalid files:", str(result.invalid_files))
    console.print(table)

    if result.files:
        details = Table(title="Local artifacts")
        details.add_column("Date")
        details.add_column("Status", no_wrap=True)
        details.add_column("Rows", justify="right")
        details.add_column("SHA-256")
        details.add_column("File")
        for item in result.files:
            details.add_row(
                item.market_date or "—",
                item.status.value,
                f"{item.row_count:,}",
                item.checksum or "—",
                str(item.path),
            )
        console.print("\n", details)
    if result.invalid_files:
        raise typer.Exit(code=1)


def _render_state_summary(summary: StateSummary, *, heading: str) -> None:
    console.print(f"[bold]{heading}[/bold]\n")
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column(justify="right")
    table.add_row("Database:", str(summary.database_path))
    table.add_row("Tracked dates:", f"{summary.tracked_dates:,}")
    for status in PersistentSyncStatus:
        if status is PersistentSyncStatus.NEVER_ATTEMPTED:
            continue
        table.add_row(
            status.value.replace("_", " ").title() + ":",
            f"{summary.counts_by_status.get(status, 0):,}",
        )
    table.add_row("Earliest tracked:", summary.earliest_tracked or "—")
    table.add_row("Latest tracked:", summary.latest_tracked or "—")
    table.add_row("Last successful sync:", summary.last_successful_sync or "—")
    console.print(table)


@app.command("status")
def status_command(
    requested_date: Annotated[
        str | None,
        typer.Option("--date", "-d", help="Inspect one tracked ISO date."),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start", "-s", help="Inclusive summary start date."),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end", "-e", help="Inclusive summary end date."),
    ] = None,
) -> None:
    """Inspect persistent synchronization state without network access."""

    if requested_date is not None and (start_date is not None or end_date is not None):
        console.print(
            "[bold red]Input error:[/bold red] --date cannot be combined with a range"
        )
        raise typer.Exit(code=2)
    if (start_date is None) != (end_date is None):
        console.print(
            "[bold red]Input error:[/bold red] --start and --end are required together"
        )
        raise typer.Exit(code=2)

    try:
        if requested_date is not None:
            validate_requested_date(requested_date, today=date.max)
        elif start_date is not None and end_date is not None:
            generate_date_range(start_date, end_date, today=date.max)
    except ValueError as exc:
        console.print(f"[bold red]Input error:[/bold red] {exc}")
        raise typer.Exit(code=2) from exc

    try:
        settings = Settings.from_env()
        repository = _repository_from_settings(settings)
        state = (
            repository.get_date_state(requested_date)
            if requested_date is not None
            else None
        )
        attempts = (
            repository.get_recent_attempts(requested_date)
            if requested_date is not None
            else ()
        )
        summary = (
            repository.summarize_range(
                start_date=start_date,
                end_date=end_date,
            )
            if requested_date is None
            else None
        )
    except (OSError, ValueError, sqlite3.Error, StateDatabaseError) as exc:
        console.print(f"[bold red]State error:[/bold red] {exc}")
        raise typer.Exit(code=1) from exc

    if requested_date is not None:
        console.print("[bold]PSX Data Sync — Date State[/bold]\n")
        if state is None:
            console.print(f"Date: {requested_date}\nStatus: NEVER_ATTEMPTED")
            return
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold")
        table.add_column()
        table.add_row("Date:", state.market_date)
        table.add_row("Status:", state.status.value)
        table.add_row("Evidence:", state.evidence_state)
        table.add_row("Lifetime attempts:", str(state.attempt_count))
        table.add_row("Successful attempts:", str(state.successful_attempt_count))
        table.add_row("Last HTTP:", str(state.last_http_status or "—"))
        table.add_row(
            "Parsed/valid/rejected:",
            (
                f"{state.parsed_row_count}/{state.valid_row_count}/"
                f"{state.rejected_row_count}"
            ),
        )
        table.add_row("SHA-256:", state.csv_checksum_sha256 or "—")
        table.add_row("CSV:", state.csv_relative_path or "—")
        table.add_row("First attempt:", state.first_attempt_at or "—")
        table.add_row("Last attempt:", state.last_attempt_at or "—")
        table.add_row("Last success:", state.last_success_at or "—")
        table.add_row("Last verified:", state.last_verified_at or "—")
        table.add_row(
            "Last error:",
            (
                f"{state.last_error_type}: {state.last_error_message}"
                if state.last_error_type and state.last_error_message
                else state.last_error_message or state.last_error_type or "—"
            ),
        )
        console.print(table)
        if attempts:
            history = Table(title="Recent network attempts")
            history.add_column("Run")
            history.add_column("Attempt", justify="right")
            history.add_column("HTTP")
            history.add_column("Classification")
            history.add_column("Result")
            for attempt in attempts:
                history.add_row(
                    attempt.run_id[:8],
                    str(attempt.attempt_number),
                    str(attempt.http_status or "—"),
                    attempt.response_classification or "—",
                    attempt.final_status,
                )
            console.print("\n", history)
        return

    assert summary is not None
    heading = "PSX Data Sync — State Summary"
    if start_date is not None and end_date is not None:
        heading += f" ({start_date} → {end_date})"
    _render_state_summary(summary, heading=heading)


if __name__ == "__main__":
    app()
