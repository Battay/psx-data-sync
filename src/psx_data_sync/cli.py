"""Typer command-line interface for PSX Data Sync."""

from __future__ import annotations

from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from .client import PSXClient
from .config import Settings
from .downloader import SingleDateDownloader
from .state import DownloadResult, DownloadStatus


app = typer.Typer(
    name="psx-data-sync",
    help="Reliable Pakistan Stock Exchange historical data downloads.",
    no_args_is_help=True,
)
console = Console()


@app.callback()
def main() -> None:
    """Download and validate PSX historical market data."""


def run_download(requested_date: str) -> DownloadResult:
    settings = Settings.from_env()
    with PSXClient(settings) as client:
        return SingleDateDownloader(settings, client).download(requested_date)


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
    except (OSError, ValueError) as exc:
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


if __name__ == "__main__":
    app()
