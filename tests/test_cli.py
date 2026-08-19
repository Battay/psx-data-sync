from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import psx_data_sync.cli as cli
import psx_data_sync.synchronizer as synchronizer
from psx_data_sync.exporter import save_canonical_csv
from psx_data_sync.parser import parse_equity_rows
from psx_data_sync.state import DownloadResult, DownloadStatus
from psx_data_sync.synchronizer import build_range_result
from psx_data_sync.validator import validate_rows


runner = CliRunner()


def test_help_lists_fetch_command() -> None:
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "fetch" in result.output
    assert "state-bootstrap" in result.output
    assert "status" in result.output


def test_invalid_date_has_distinct_exit_code(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(tmp_path))

    result = runner.invoke(cli.app, ["fetch", "--date", "2026-8-5"])

    assert result.exit_code == 2
    assert "INVALID_DATE" in result.output
    assert "YYYY-MM-DD" in result.output


def test_mocked_success_prints_summary(monkeypatch, tmp_path: Path) -> None:
    expected = DownloadResult(
        requested_date="2026-08-05",
        status=DownloadStatus.TRADING_DATA,
        http_status=200,
        attempts=2,
        response_bytes=2048,
        parsed_row_count=2,
        valid_row_count=2,
        elapsed_ms=123,
        saved_path=tmp_path / "market_2026-08-05.csv",
        checksum="abc123",
    )
    monkeypatch.setattr(cli, "run_download", lambda _: expected)

    result = runner.invoke(cli.app, ["fetch", "-d", "2026-08-05"])

    assert result.exit_code == 0
    assert "TRADING_DATA" in result.output
    assert "abc123" in result.output


def test_mocked_failure_returns_nonzero(monkeypatch) -> None:
    expected = DownloadResult(
        requested_date="2026-08-05",
        status=DownloadStatus.TEMPORARY_FAILURE,
        attempts=3,
        error="timed out",
    )
    monkeypatch.setattr(cli, "run_download", lambda _: expected)

    result = runner.invoke(cli.app, ["fetch", "--date", "2026-08-05"])

    assert result.exit_code == 1
    assert "TEMPORARY_FAILURE" in result.output
    assert "timed out" in result.output


def range_result(statuses: tuple[DownloadStatus, ...]):
    dates = tuple(date(2026, 8, 4 + index) for index in range(len(statuses)))
    results = tuple(
        DownloadResult(
            requested_date=requested.isoformat(),
            status=status,
            attempts=1,
            valid_row_count=3 if status is DownloadStatus.TRADING_DATA else 0,
        )
        for requested, status in zip(dates, statuses, strict=True)
    )
    return build_range_result(dates, 2, 1000, results)


def test_fetch_range_mocked_success_renders_summary(monkeypatch) -> None:
    expected = range_result(
        (DownloadStatus.TRADING_DATA, DownloadStatus.ALREADY_PRESENT)
    )

    def mocked(*_, **__):
        return expected

    monkeypatch.setattr(cli, "run_range_download", mocked)

    result = runner.invoke(
        cli.app,
        [
            "fetch-range",
            "-s",
            "2026-08-04",
            "-e",
            "2026-08-05",
            "--workers",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "Range Fetch" in result.output
    assert "TRADING_DATA" in result.output
    assert "ALREADY_PRESENT" in result.output
    assert "2.00 dates/s" in result.output


def test_fetch_range_failure_returns_one(monkeypatch) -> None:
    expected = range_result(
        (DownloadStatus.TRADING_DATA, DownloadStatus.TEMPORARY_FAILURE)
    )
    monkeypatch.setattr(cli, "run_range_download", lambda *_, **__: expected)

    result = runner.invoke(
        cli.app,
        ["fetch-range", "-s", "2026-08-04", "-e", "2026-08-05"],
    )

    assert result.exit_code == 1
    assert "Failed dates" in result.output


def test_fetch_range_empty_result_has_distinct_exit(monkeypatch) -> None:
    expected = range_result((DownloadStatus.NON_TRADING_OR_EMPTY,))
    monkeypatch.setattr(cli, "run_range_download", lambda *_, **__: expected)

    result = runner.invoke(
        cli.app,
        ["fetch-range", "-s", "2026-08-05", "-e", "2026-08-05"],
    )

    assert result.exit_code == 3
    assert "Empty/unresolved dates" in result.output


def test_fetch_range_rejects_worker_above_cap() -> None:
    result = runner.invoke(
        cli.app,
        [
            "fetch-range",
            "-s",
            "2026-08-04",
            "-e",
            "2026-08-05",
            "--workers",
            "17",
        ],
    )

    assert result.exit_code == 2


def test_fetch_range_rejects_reversed_dates() -> None:
    result = runner.invoke(
        cli.app,
        ["fetch-range", "-s", "2026-08-05", "-e", "2026-08-04"],
    )

    assert result.exit_code == 2
    assert "start date" in result.output


def write_local_csv(output_dir: Path, market_date: date, fixture_bytes) -> None:
    rows = validate_rows(
        parse_equity_rows(fixture_bytes("valid_market.html"))
    ).valid_rows
    save_canonical_csv(rows, market_date, output_dir)


def configure_state_paths(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    raw_dir = tmp_path / "raw"
    database = tmp_path / "state" / "psx_sync.db"
    monkeypatch.setenv("PSX_RAW_OUTPUT_DIR", str(raw_dir))
    monkeypatch.setenv("PSX_STATE_DB_PATH", str(database))
    return raw_dir, database


def test_bootstrap_and_status_cli_are_network_free(
    tmp_path: Path, monkeypatch, fixture_bytes
) -> None:
    raw_dir, database = configure_state_paths(monkeypatch, tmp_path)
    write_local_csv(raw_dir, date(2026, 8, 5), fixture_bytes)

    bootstrap = runner.invoke(cli.app, ["state-bootstrap"])
    summary = runner.invoke(cli.app, ["status"])
    detail = runner.invoke(cli.app, ["status", "--date", "2026-08-05"])
    ranged = runner.invoke(
        cli.app,
        ["status", "--start", "2026-08-01", "--end", "2026-08-31"],
    )

    assert bootstrap.exit_code == 0
    assert "Discovered files:" in bootstrap.output
    assert "Newly indexed:" in bootstrap.output
    assert "VERIFIED_TRADING_DATA" in bootstrap.output
    assert database.exists()
    assert summary.exit_code == 0
    assert "Tracked dates:" in summary.output
    assert "Verified Trading Data:" in summary.output
    assert detail.exit_code == 0
    assert "VERIFIED_TRADING_DATA" in detail.output
    assert "Lifetime attempts:" in detail.output
    assert "0" in detail.output
    assert ranged.exit_code == 0
    assert "2026-08-01" in ranged.output
    assert "2026-08-31" in ranged.output


def test_status_date_and_range_options_are_mutually_exclusive(
    tmp_path: Path, monkeypatch
) -> None:
    configure_state_paths(monkeypatch, tmp_path)

    mixed = runner.invoke(
        cli.app,
        [
            "status",
            "--date",
            "2026-08-05",
            "--start",
            "2026-08-01",
            "--end",
            "2026-08-31",
        ],
    )
    incomplete = runner.invoke(cli.app, ["status", "--start", "2026-08-01"])

    assert mixed.exit_code == 2
    assert "cannot be combined" in mixed.output
    assert incomplete.exit_code == 2
    assert "required together" in incomplete.output


def test_state_aware_single_fetch_skips_http(
    tmp_path: Path, monkeypatch, fixture_bytes
) -> None:
    raw_dir, _ = configure_state_paths(monkeypatch, tmp_path)
    write_local_csv(raw_dir, date(2026, 8, 5), fixture_bytes)
    assert runner.invoke(cli.app, ["state-bootstrap"]).exit_code == 0

    class NoNetworkClient:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def fetch(self, _requested_date):
            raise AssertionError("verified local fetch reached HTTP")

    monkeypatch.setattr(cli, "PSXClient", NoNetworkClient)
    result = runner.invoke(cli.app, ["fetch", "--date", "2026-08-05"])

    assert result.exit_code == 0
    assert "ALREADY_PRESENT" in result.output
    assert "Attempts:" in result.output


def test_state_aware_range_fetch_skips_all_http(
    tmp_path: Path, monkeypatch, fixture_bytes
) -> None:
    raw_dir, _ = configure_state_paths(monkeypatch, tmp_path)
    write_local_csv(raw_dir, date(2026, 8, 4), fixture_bytes)
    write_local_csv(raw_dir, date(2026, 8, 5), fixture_bytes)
    assert runner.invoke(cli.app, ["state-bootstrap"]).exit_code == 0

    class NoNetworkAsyncClient:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def fetch(self, _requested_date):
            raise AssertionError("verified range reached HTTP")

    monkeypatch.setattr(synchronizer, "AsyncPSXClient", NoNetworkAsyncClient)
    result = runner.invoke(
        cli.app,
        [
            "fetch-range",
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-05",
            "--workers",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "Network-fetched dates:" in result.output
    assert "Locally skipped dates:" in result.output
    assert result.output.count("ALREADY_PRESENT") >= 2
