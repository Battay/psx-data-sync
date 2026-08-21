"""Typer command-line interface for PSX Data Sync."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import sys
import time
from collections.abc import Mapping
from dataclasses import fields, is_dataclass, replace
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Annotated, Any

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
    ReconciliationRangeResult,
    StateSummary,
)
from .importer import (
    BatchImportResult,
    LocalFileImportResult,
    LocalImportAction,
    import_local_csv_directory,
)
from .parquet_sync import (
    DateParquetSyncResult,
    ParquetExportAction,
    RangeParquetSyncResult,
    sync_parquet_range,
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


def run_reconciliation(
    start_date: str,
    end_date: str,
    workers: int,
    *,
    apply: bool = False,
    force_recheck: bool = False,
    settings: Settings | None = None,
) -> ReconciliationRangeResult:
    """Run the D4 service while keeping orchestration out of the CLI command."""

    resolved_settings = settings or Settings.from_env()
    repository = StateRepository(
        resolved_settings.state_db_path,
        source_endpoint=resolved_settings.historical_url,
    )
    repository.initialize()

    # Imported lazily so the D1-D3 commands remain usable independently of the
    # reconciliation orchestration module during installation and migration.
    from .reconciliation import run_reconciliation as execute_reconciliation

    return execute_reconciliation(
        resolved_settings,
        repository,
        start_date,
        end_date,
        apply=apply,
        force_recheck=force_recheck,
        workers=workers,
    )


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
    summary.add_row(
        "Confirmed non-trading:",
        f"{counts.get(DownloadStatus.CONFIRMED_NON_TRADING, 0):,}",
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


def _json_value(value: Any) -> Any:
    """Convert result dataclasses into deterministic JSON-compatible values."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(_json_value(key)): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _filtered_reconciliation_results(
    result: ReconciliationRangeResult,
    *,
    only_problems: bool,
    status: PersistentSyncStatus | None,
) -> tuple:
    """Filter displayed dates without changing whole-range summary semantics."""

    return tuple(
        item
        for item in result.results
        if (not only_problems or item.has_problem)
        and (status is None or item.reconciled_status is status)
    )


def reconciliation_result_to_dict(
    result: ReconciliationRangeResult,
    *,
    only_problems: bool = False,
    status: PersistentSyncStatus | None = None,
) -> dict[str, Any]:
    """Build the stable machine-readable reconciliation report."""

    displayed = _filtered_reconciliation_results(
        result,
        only_problems=only_problems,
        status=status,
    )
    date_results: list[dict[str, Any]] = []
    for item in displayed:
        serialized = _json_value(item)
        assert isinstance(serialized, dict)
        serialized.pop("state_snapshot_exists", None)
        serialized.pop("state_record_updated_at", None)
        serialized["resolved"] = item.resolved
        serialized["has_problem"] = item.has_problem
        date_results.append(serialized)

    return {
        "report_schema_version": 1,
        "run_id": result.run_id,
        "range": {
            "start_date": result.start_date,
            "end_date": result.end_date,
            "requested_date_count": len(result.requested_dates),
            "displayed_date_count": len(displayed),
        },
        "mode": result.mode.value,
        "policy_version": result.policy_version,
        "complete": result.complete,
        "resolution_percentage": result.resolution_percentage,
        "summary": {
            "counts_by_status": _json_value(result.counts_by_status),
            "counts_by_action": _json_value(result.counts_by_action),
            "verified_count": result.verified_count,
            "confirmed_non_trading_count": result.confirmed_non_trading_count,
            "never_attempted_count": result.never_attempted_count,
            "unresolved_count": result.unresolved_count,
            "failure_count": result.failure_count,
            "file_health_issue_count": result.file_health_issue_count,
            "network_recheck_planned_count": (
                result.network_recheck_planned_count
            ),
            "network_recheck_eligible_count": sum(
                item.network_recheck_required and item.recheck_eligible_now
                for item in result.results
            ),
            "network_recheck_count": result.network_recheck_count,
            "local_repair_count": result.local_repair_count,
            "local_repair_remaining_count": result.local_repair_count,
            "manual_review_count": result.manual_review_count,
            "status_transition_count": result.status_transition_count,
        },
        "activity": {
            "network_rechecked_dates": list(result.network_rechecked_dates),
            "staged_repair_dates": list(result.staged_repair_dates),
            "promoted_repair_dates": list(result.promoted_repair_dates),
            "duration_ms": result.duration_ms,
        },
        "filters": {
            "only_problems": only_problems,
            "status": status.value if status is not None else None,
        },
        "results": date_results,
        "warnings": list(result.warnings),
    }


def render_reconciliation_result(
    result: ReconciliationRangeResult,
    *,
    only_problems: bool = False,
    status: PersistentSyncStatus | None = None,
) -> None:
    """Render a concise whole-range summary and filtered date-level plan."""

    displayed = _filtered_reconciliation_results(
        result,
        only_problems=only_problems,
        status=status,
    )
    console.print("[bold]PSX Data Sync — Reconciliation[/bold]")
    console.print(f"Range: {result.start_date} → {result.end_date}")
    console.print(f"Mode: {result.mode.value.replace('_', ' ')}")
    console.print(f"Policy: {result.policy_version}")
    console.print(f"Run ID: {result.run_id}\n")

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="bold")
    summary.add_column(justify="right")
    summary.add_row("Verified trading:", f"{result.verified_count:,}")
    summary.add_row(
        "Confirmed non-trading:",
        f"{result.confirmed_non_trading_count:,}",
    )
    summary.add_row("Never attempted:", f"{result.never_attempted_count:,}")
    summary.add_row("Unresolved:", f"{result.unresolved_count:,}")
    summary.add_row("Failures:", f"{result.failure_count:,}")
    summary.add_row("File health issues:", f"{result.file_health_issue_count:,}")
    summary.add_row(
        "Network rechecks required:",
        f"{result.network_recheck_planned_count:,}",
    )
    summary.add_row(
        "Network rechecks eligible now:",
        f"{sum(item.network_recheck_required and item.recheck_eligible_now for item in result.results):,}",
    )
    summary.add_row(
        "Network rechecks performed:",
        f"{result.network_recheck_count:,}",
    )
    summary.add_row(
        "Local repairs remaining:", f"{result.local_repair_count:,}"
    )
    summary.add_row("Manual review:", f"{result.manual_review_count:,}")
    summary.add_row("Status transitions:", f"{result.status_transition_count:,}")
    summary.add_row(
        "Completeness:",
        (
            f"{'COMPLETE' if result.complete else 'INCOMPLETE'} "
            f"({result.resolution_percentage:.2f}%)"
        ),
    )
    console.print(summary)

    if only_problems or status is not None:
        filters: list[str] = []
        if only_problems:
            filters.append("problems only")
        if status is not None:
            filters.append(f"status={status.value}")
        console.print(
            "\n[dim]Displayed dates filtered by " + ", ".join(filters) + ".[/dim]"
        )

    if displayed:
        outcomes = Table(title="Date-level reconciliation plan", show_lines=False)
        outcomes.add_column("Date", no_wrap=True)
        outcomes.add_column("Previous", no_wrap=True)
        outcomes.add_column("Reconciled", no_wrap=True)
        outcomes.add_column("File", no_wrap=True)
        outcomes.add_column("Checksum", no_wrap=True)
        outcomes.add_column("Action", no_wrap=True)
        outcomes.add_column("Recheck", no_wrap=True)
        outcomes.add_column("Reason")
        for item in displayed:
            if item.network_recheck_required:
                recheck = "eligible" if item.recheck_eligible_now else "cooldown"
            else:
                recheck = "—"
            detail = "; ".join(item.reasons) or "; ".join(item.warnings) or "—"
            outcomes.add_row(
                item.market_date,
                item.previous_status.value,
                item.reconciled_status.value,
                item.file_state.value,
                item.checksum_state.value,
                item.action_required.value,
                recheck,
                detail,
            )
        console.print("\n", outcomes)
    else:
        console.print("\nNo dates match the selected display filters.")

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
    if result.successful or result.status is DownloadStatus.CONFIRMED_NON_TRADING:
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


def _render_reconcile_error(
    category: str,
    message: str,
    *,
    json_output: bool,
) -> None:
    if json_output:
        typer.echo(
            json.dumps(
                {"category": category, "error": message},
                sort_keys=True,
            )
        )
        return
    console.print(f"[bold red]{category}:[/bold red] {message}")


@app.command("reconcile")
def reconcile_command(
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
    apply_changes: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Apply safe repairs and eligible targeted network rechecks.",
        ),
    ] = False,
    force_recheck: Annotated[
        bool,
        typer.Option(
            "--force-recheck",
            help="Bypass cooldowns for eligible unresolved/failure states.",
        ),
    ] = False,
    only_problems: Annotated[
        bool,
        typer.Option(
            "--only-problems",
            help="Show only dates needing action; summary remains whole-range.",
        ),
    ] = False,
    status_filter: Annotated[
        PersistentSyncStatus | None,
        typer.Option(
            "--status",
            help="Show only dates with this reconciled status.",
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a clean machine-readable JSON report.",
        ),
    ] = False,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            "-w",
            help=f"Concurrent rechecks (1–{MAX_RANGE_WORKERS}).",
            min=1,
            max=MAX_RANGE_WORKERS,
        ),
    ] = None,
) -> None:
    """Audit a range and optionally apply conservative reconciliation actions."""

    try:
        generate_date_range(start_date, end_date)
    except ValueError as exc:
        _render_reconcile_error("Input error", str(exc), json_output=json_output)
        raise typer.Exit(code=2) from exc

    if force_recheck and not apply_changes:
        message = "--force-recheck requires --apply"
        _render_reconcile_error("Input error", message, json_output=json_output)
        raise typer.Exit(code=2)

    try:
        settings = Settings.from_env()
    except ValueError as exc:
        _render_reconcile_error(
            "Configuration error",
            str(exc),
            json_output=json_output,
        )
        raise typer.Exit(code=1) from exc

    try:
        resolved_workers = validate_workers(
            settings.range_workers if workers is None else workers,
            settings,
        )
    except ValueError as exc:
        _render_reconcile_error("Input error", str(exc), json_output=json_output)
        raise typer.Exit(code=2) from exc

    try:
        result = run_reconciliation(
            start_date,
            end_date,
            resolved_workers,
            apply=apply_changes,
            force_recheck=force_recheck,
            settings=settings,
        )
    except KeyboardInterrupt as exc:
        _render_reconcile_error(
            "Interrupted",
            "completed atomic work and audit evidence were preserved",
            json_output=json_output,
        )
        raise typer.Exit(code=130) from exc
    except Exception as exc:
        _render_reconcile_error(
            "Reconciliation error",
            str(exc),
            json_output=json_output,
        )
        raise typer.Exit(code=1) from exc

    if json_output:
        typer.echo(
            json.dumps(
                reconciliation_result_to_dict(
                    result,
                    only_problems=only_problems,
                    status=status_filter,
                ),
                indent=2,
                sort_keys=True,
            )
        )
    else:
        render_reconciliation_result(
            result,
            only_problems=only_problems,
            status=status_filter,
        )

    if not result.complete:
        raise typer.Exit(code=3)


def _render_parquet_error(
    category: str,
    message: str,
    *,
    json_output: bool,
) -> None:
    if json_output:
        typer.echo(
            json.dumps(
                {"category": category, "error": message},
                sort_keys=True,
            )
        )
        return
    console.print(f"[bold red]{category}:[/bold red] {message}")


def parquet_range_result_to_dict(result: RangeParquetSyncResult) -> dict[str, Any]:
    return {
        "start_date": result.start_date,
        "end_date": result.end_date,
        "mode": "DRY_RUN" if result.dry_run else "APPLY",
        "requested_count": result.requested_count,
        "eligible_count": result.eligible_count,
        "current_count": result.current_count,
        "create_count": result.create_count,
        "stale_count": result.stale_count,
        "corrupt_count": result.corrupt_count,
        "reindexed_count": result.reindexed_count,
        "excluded_non_trading_count": result.excluded_non_trading_count,
        "excluded_unresolved_count": result.excluded_unresolved_count,
        "excluded_failure_count": result.excluded_failure_count,
        "excluded_file_issue_count": result.excluded_file_issue_count,
        "source_invalid_count": result.source_invalid_count,
        "failed_count": result.failed_count,
        "written_or_rebuilt_count": result.written_or_rebuilt_count,
        "synchronized": result.synchronized,
        "synchronization_percentage": round(result.synchronization_percentage, 2),
        "duration_ms": round(result.duration_ms, 2),
        "results": [
            {
                "market_date": r.market_date,
                "source_status": r.source_status.value if r.source_status else None,
                "action": r.action.value if hasattr(r.action, "value") else str(r.action),
                "export_status_before": (
                    r.export_status_before.value if r.export_status_before else None
                ),
                "export_status_planned": (
                    r.export_status_planned.value if r.export_status_planned else None
                ),
                "export_status_after": (
                    r.export_status_after.value if r.export_status_after else None
                ),
                "eligible": r.eligible,
                "source_csv_path": str(r.source_csv_path) if r.source_csv_path else None,
                "source_checksum": r.source_checksum,
                "source_row_count": r.source_row_count,
                "parquet_path": str(r.parquet_path) if r.parquet_path else None,
                "parquet_checksum": r.parquet_checksum,
                "parquet_row_count": r.parquet_row_count,
                "dry_run": r.dry_run,
                "rebuilt_or_written": r.rebuilt_or_written,
                "synchronized": r.synchronized,
                "warnings": list(r.warnings),
                "error": r.error,
            }
            for r in result.results
        ],
        "warnings": list(result.warnings),
    }


def render_parquet_range_result(result: RangeParquetSyncResult) -> None:
    mode_str = "DRY_RUN (planning only)" if result.dry_run else "APPLY (actual export)"
    console.print(
        f"[bold]PSX Data Sync — Parquet Export[/bold] ({result.start_date} → {result.end_date})\n"
    )
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column(justify="right")

    table.add_row("Mode:", mode_str)
    table.add_row("Requested dates:", f"{result.requested_count:,}")
    table.add_row("Eligible dates:", f"{result.eligible_count:,}")
    table.add_row("Current (no-op):", f"{result.current_count:,}")
    table.add_row("Create (new):", f"{result.create_count:,}")
    table.add_row("Stale (rebuild):", f"{result.stale_count:,}")
    table.add_row("Corrupt (rebuild):", f"{result.corrupt_count:,}")
    table.add_row("Reindex:", f"{result.reindexed_count:,}")
    table.add_row("Excluded non-trading:", f"{result.excluded_non_trading_count:,}")
    table.add_row("Excluded unresolved:", f"{result.excluded_unresolved_count:,}")
    table.add_row("Excluded failures:", f"{result.excluded_failure_count:,}")
    table.add_row("Excluded file issues:", f"{result.excluded_file_issue_count:,}")
    table.add_row("Source invalid:", f"{result.source_invalid_count:,}")
    table.add_row("Failed:", f"{result.failed_count:,}")
    table.add_row("Written / rebuilt:", f"{result.written_or_rebuilt_count:,}")
    table.add_row("Synchronization percentage:", f"{result.synchronization_percentage:.1f}%")
    table.add_row("Synchronized:", "Yes" if result.synchronized else "No")
    table.add_row("Duration:", f"{result.duration_ms:.2f} ms")
    console.print(table)

    if result.results:
        details = Table(title="Per-date Parquet status")
        details.add_column("Date")
        details.add_column("Source Status")
        details.add_column("Action")
        details.add_column("Before")
        details.add_column("After / Planned")
        details.add_column("Written", justify="center")

        for item in result.results:
            details.add_row(
                item.market_date,
                item.source_status.value if item.source_status else "—",
                item.action.value if hasattr(item.action, "value") else str(item.action),
                item.export_status_before.value if item.export_status_before else "—",
                (
                    (item.export_status_after.value if item.export_status_after else "—")
                    if not result.dry_run
                    else (
                        item.export_status_planned.value
                        if item.export_status_planned
                        else "—"
                    )
                ),
                "✓" if item.rebuilt_or_written else "—",
            )
        console.print("\n", details)


@app.command("export-parquet")
def export_parquet_command(
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
    apply_changes: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Apply Parquet exports and database state updates.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Perform planning and validation only without mutating files or database.",
        ),
    ] = False,
    rebuild: Annotated[
        bool,
        typer.Option(
            "--rebuild",
            help="Force regeneration of current Parquet partitions (requires --apply).",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a clean machine-readable JSON report.",
        ),
    ] = False,
) -> None:
    """Synchronize derived Parquet partitions for a date range."""

    try:
        generate_date_range(start_date, end_date)
    except ValueError as exc:
        _render_parquet_error("Input error", str(exc), json_output=json_output)
        raise typer.Exit(code=2) from exc

    if rebuild and not apply_changes:
        _render_parquet_error(
            "Input error",
            "--rebuild requires --apply",
            json_output=json_output,
        )
        raise typer.Exit(code=2)

    effective_dry_run = True if (dry_run or not apply_changes) else False

    try:
        settings = Settings.from_env()
    except ValueError as exc:
        _render_parquet_error(
            "Configuration error",
            str(exc),
            json_output=json_output,
        )
        raise typer.Exit(code=1) from exc

    try:
        repository = _repository_from_settings(settings)
        result = sync_parquet_range(
            repository,
            start_date,
            end_date,
            output_root=settings.raw_output_dir.parent / "parquet",
            dry_run=effective_dry_run,
            rebuild=rebuild,
        )
    except KeyboardInterrupt as exc:
        _render_parquet_error(
            "Interrupted",
            "operation cancelled by user",
            json_output=json_output,
        )
        raise typer.Exit(code=130) from exc
    except Exception as exc:
        _render_parquet_error(
            "Parquet export error",
            str(exc),
            json_output=json_output,
        )
        raise typer.Exit(code=1) from exc

    if json_output:
        typer.echo(
            json.dumps(
                parquet_range_result_to_dict(result),
                indent=2,
                sort_keys=True,
            )
        )
    else:
        render_parquet_range_result(result)

    if not result.synchronized:
        raise typer.Exit(code=3)


def _render_import_error(
    category: str,
    message: str,
    *,
    json_output: bool,
) -> None:
    if json_output:
        typer.echo(
            json.dumps(
                {"category": category, "error": message},
                sort_keys=True,
            )
        )
        return
    console.print(f"[bold red]{category}:[/bold red] {message}")


def batch_import_result_to_dict(result: BatchImportResult) -> dict[str, Any]:
    return {
        "source_dir": str(result.source_dir),
        "destination_dir": str(result.destination_dir),
        "mode": "DRY_RUN" if result.dry_run else "APPLY",
        "discovered_count": result.discovered_count,
        "candidate_count": result.candidate_count,
        "importable_count": result.importable_count,
        "imported_count": result.imported_count,
        "already_present_count": result.already_present_count,
        "invalid_count": result.invalid_count,
        "conflict_count": result.conflict_count,
        "unsupported_count": result.unsupported_count,
        "failed_count": result.failed_count,
        "duration_ms": round(result.duration_ms, 2),
        "results": [
            {
                "source_path": str(r.source_path),
                "market_date": r.market_date,
                "action": r.action.value if hasattr(r.action, "value") else str(r.action),
                "valid": r.valid,
                "row_count": r.row_count,
                "rejected_row_count": r.rejected_row_count,
                "source_checksum": r.source_checksum,
                "destination_path": (
                    str(r.destination_path) if r.destination_path else None
                ),
                "destination_checksum": r.destination_checksum,
                "imported": r.imported,
                "warnings": list(r.warnings),
                "error": r.error,
            }
            for r in result.results
        ],
    }


def render_batch_import_result(result: BatchImportResult) -> None:
    mode_str = "DRY_RUN (planning only)" if result.dry_run else "APPLY (actual import)"
    console.print("[bold]PSX Data Sync — Local Historical CSV Import[/bold]\n")
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold")
    table.add_column(justify="right")

    table.add_row("Source directory:", str(result.source_dir))
    table.add_row("Destination directory:", str(result.destination_dir))
    table.add_row("Mode:", mode_str)
    table.add_row("Discovered files:", f"{result.discovered_count:,}")
    table.add_row("Candidate CSVs:", f"{result.candidate_count:,}")
    table.add_row(
        "Imported (new):" if not result.dry_run else "Importable (new):",
        f"{result.imported_count if not result.dry_run else result.importable_count:,}",
    )
    table.add_row("Already present:", f"{result.already_present_count:,}")
    table.add_row("Invalid source files:", f"{result.invalid_count:,}")
    table.add_row("Conflicts:", f"{result.conflict_count:,}")
    table.add_row("Unsupported files:", f"{result.unsupported_count:,}")
    table.add_row("Failed:", f"{result.failed_count:,}")
    table.add_row("Duration:", f"{result.duration_ms:.2f} ms")
    console.print(table)

    if result.results:
        details = Table(title="Import candidate files")
        details.add_column("Source File")
        details.add_column("Date")
        details.add_column("Action")
        details.add_column("Rows", justify="right")
        details.add_column("Imported", justify="center")

        for item in result.results:
            details.add_row(
                item.source_path.name,
                item.market_date or "—",
                item.action.value if hasattr(item.action, "value") else str(item.action),
                f"{item.row_count:,}" if item.row_count is not None else "—",
                "✓" if item.imported else "—",
            )
        console.print("\n", details)


@app.command("import-csv")
def import_csv_command(
    source: Annotated[
        Path,
        typer.Option(
            "--source",
            "-src",
            help="Directory containing historical canonical CSV files.",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ],
    apply_changes: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Perform actual atomic file imports and state database indexing.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Perform planning and validation only without mutating files or database.",
        ),
    ] = False,
    recursive: Annotated[
        bool,
        typer.Option(
            "--recursive",
            "-r",
            help="Scan source directory recursively for CSV files.",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit a clean machine-readable JSON report.",
        ),
    ] = False,
) -> None:
    """Import historical canonical CSV files from a local directory."""

    effective_dry_run = True if (dry_run or not apply_changes) else False

    try:
        settings = Settings.from_env()
    except ValueError as exc:
        _render_import_error("Configuration error", str(exc), json_output=json_output)
        raise typer.Exit(code=1) from exc

    try:
        repository = _repository_from_settings(settings)
        result = import_local_csv_directory(
            repository,
            source,
            destination_dir=settings.raw_output_dir,
            dry_run=effective_dry_run,
            recursive=recursive,
        )
    except FileNotFoundError as exc:
        _render_import_error("Input error", str(exc), json_output=json_output)
        raise typer.Exit(code=2) from exc
    except KeyboardInterrupt as exc:
        _render_import_error(
            "Interrupted", "operation cancelled by user", json_output=json_output
        )
        raise typer.Exit(code=130) from exc
    except Exception as exc:
        _render_import_error("Import error", str(exc), json_output=json_output)
        raise typer.Exit(code=1) from exc

    if json_output:
        typer.echo(
            json.dumps(
                batch_import_result_to_dict(result),
                indent=2,
                sort_keys=True,
            )
        )
    else:
        render_batch_import_result(result)

    if result.invalid_count or result.conflict_count or result.failed_count:
        raise typer.Exit(code=1)


def _repository_from_settings(settings: Settings) -> StateRepository:
    raw_dir = settings.raw_output_dir.resolve()
    project_root = (
        raw_dir.parent.parent
        if raw_dir.name == "raw" and raw_dir.parent.name == "data"
        else raw_dir.parent
    )
    repository = StateRepository(
        settings.state_db_path,
        project_root=project_root,
        raw_output_dir=settings.raw_output_dir,
        source_endpoint=settings.historical_url,
    )
    repository.initialize()
    return repository


@app.command("gui")
def gui_command() -> None:
    """Launch the PSX Data Sync desktop GUI application."""

    from .gui import main as gui_main

    sys.exit(gui_main())


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
