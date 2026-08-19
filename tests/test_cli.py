from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import psx_data_sync.cli as cli
from psx_data_sync.state import DownloadResult, DownloadStatus
from psx_data_sync.synchronizer import build_range_result


runner = CliRunner()


def test_help_lists_fetch_command() -> None:
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "fetch" in result.output


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
